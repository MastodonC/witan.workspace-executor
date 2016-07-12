(ns witan.workspace-executor.core
  (:require [manifold.deferred :as d]
            [schema.core :as s]
            [clojure.core.async :as async]
            [clojure.stacktrace :as st]
            [clojure.walk :as walk]
            [rhizome.viz :as viz]
            [taoensso.timbre :as log]
            [witan.workspace-executor.schema :as as]
            [witan.workspace-executor.utils :as utils]))

(defn- get-catalog-entry
  [catalog id]
  (some #(when (= (:witan/name %) id) %) catalog))

(defn- get-contract
  [{:keys [catalog contracts]} id]
  (let [catalog-entry          (get-catalog-entry catalog id)
        fnc                    (:witan/fn catalog-entry)
        version                (:witan/version catalog-entry)]
    (some #(when (and (= (:witan/fn %) fnc) (= (:witan/version %) version)) %) contracts)))

(def branch?
  #(and (vector? %) (= 3 (count %))))

(s/defn workflow->long-hand-workflow
  [wf :- as/Workflow]
  (let [fnc-froms   (fn [k ws]
                      (->> ws
                           (filter (fn [y] (some #{k} (-> y (second) :to))))
                           (reduce (fn [a c] (conj a (first c))) [])
                           (not-empty)))
        predicates  (->> wf
                         (map second)
                         (filter branch?))
        with-to     (->> wf
                         (reduce (fn [x [from to]]
                                   (let [to' (if (branch? to) (first to) to)]
                                     (assoc x from (conj (get x from []) to')))) {})
                         (map (fn [[n t]] (hash-map n {:to t})))
                         (into {}))
        with-to-with-preds (->> predicates
                                (reduce (fn [a [pred break loop]]
                                          (-> a
                                              (assoc-in [pred :to] (vec (concat (get-in a [pred :to] []) [break loop])))
                                              (assoc-in [pred :pred?] true))) with-to))
        with-leaves (->> (set (flatten wf))
                         (reduce (fn [a c] (if (contains? a c) a (conj a (hash-map c {})))) (into {} with-to-with-preds)))
        with-froms  (reduce (fn [a x] (if-let [froms (fnc-froms (first x) with-leaves)]
                                        (conj a (update-in x [1] assoc :from froms))
                                        (conj a x))) [] with-leaves)]
    (into (sorted-map) with-froms)))

(s/defn validate-workspace
  [{:keys [workflow contracts catalog] :as workspace} :- as/Workspace]
  (let [workflow* (set (flatten workflow))
        nodes (workflow->long-hand-workflow workflow)
        outputs-from-parents-fn (fn [from] (mapcat (fn [x]
                                                     (let [parent-ce          (get-catalog-entry catalog x)
                                                           parent-contract    (get-contract workspace x)
                                                           parent-con-outputs (set (map :witan/key
                                                                                        (:witan/outputs parent-contract)))
                                                           output-mappings    (map (juxt :witan/output-src-key
                                                                                         :witan/output-dest-key)
                                                                                   (:witan/outputs parent-ce))]
                                                       (reduce (fn [a [from to]]
                                                                 (-> a
                                                                     (disj from)
                                                                     (conj to))) parent-con-outputs
                                                               output-mappings)))
                                                   from))]

    ;;
    ;; Does the contract have duplicates in input or output keys?
    ;;
    (let [duplicate-contract-inputs
          (into []
                (remove (fn [{:keys [witan/inputs]}]
                          (let [input-set (set (map :witan/key inputs))]
                            (= (count input-set) (count inputs)))))
                contracts)]
      (when (not-empty duplicate-contract-inputs)
        (throw
         (IllegalArgumentException.
          (str "One or more contracts have duplicated inputs: " duplicate-contract-inputs)))))
    (let [duplicate-contract-outputs
          (into []
                (remove (fn [{:keys [witan/outputs]}]
                          (let [output-set (set (map :witan/key outputs))]
                            (= (count output-set) (count outputs)))))
                contracts)]
      (when (not-empty duplicate-contract-outputs)
        (throw
         (IllegalArgumentException.
          (str "One or more contracts have duplicated outputs: " duplicate-contract-outputs)))))

    ;;
    ;; Does the catalog have duplicates?
    ;;
    (let [duplicate-catalog-entries
          (utils/find-dupes
           (into []
                 (map :witan/name)
                 catalog))]
      (when (not-empty duplicate-catalog-entries)
        (throw
         (IllegalArgumentException.
          (str "One or more catalog entries are duplicated: " duplicate-catalog-entries)))))

    ;;
    ;; Are all workflow nodes represented in the catalog?
    ;;
    (let [catalog-names
          (into []
                (map :witan/name)
                catalog)
          diff (not-empty (clojure.set/difference workflow* catalog-names))]
      (when diff
        (throw
         (IllegalArgumentException.
          (str "One or more workflow entries are unrepresented in the catalog: " diff)))))

    ;;
    ;; Do all functions in the catalog have a contract for the specified version?
    ;;
    (let [missing-contracts
          (into []
                (comp
                 (keep (fn [node] (some #(when (= (:witan/name %) node) ((juxt :witan/fn :witan/version) %)) catalog)))
                 (keep (fn [[fn-name fn-version]]
                         (some #(when-not (or (= (:witan/fn %) fn-name)
                                              (= (:witan/version %) fn-version)) [fn-name fn-version]) contracts))))
                workflow*)]
      (when (not-empty missing-contracts)
        (throw
         (IllegalArgumentException.
          (str "There are no contracts for the following function + version combinations: " missing-contracts)))))

    ;;
    ;; Does each catalog-entry list all inputs from the contract?
    ;;
    (let [missing-inputs
          (into []
                (keep (fn [[k node]]
                        (let [ce       (get-catalog-entry catalog k)
                              contract (get-contract workspace k)
                              from-ce (map (fn [{:keys [witan/input-src-key
                                                        witan/input-dest-key
                                                        witan/input-src-fn]}]
                                             (if input-src-fn
                                               input-dest-key
                                               input-src-key)) (:witan/inputs ce))
                              from-contract (map :witan/key (:witan/inputs contract))]
                          (when-not (= (set from-contract) (set from-ce))
                            (hash-map k {:has from-ce :expects from-contract})))))
                nodes)]
      (when (not-empty missing-inputs)
        (throw
         (IllegalArgumentException.
          (str "The following nodes are either missing a reference to an input or provide an unexpected input: " missing-inputs)))))

    ;;
    ;; Will each node have its inputs provided by a parent?
    ;;
    (let [missing-inputs-from-parents
          (into []
                (keep (fn [[k node]]
                        (let [ce       (get-catalog-entry catalog k)
                              contract (get-contract workspace k)
                              inputs   (map :witan/key (:witan/inputs contract))
                              outputs-from-parents (when (:from node) (outputs-from-parents-fn (:from node)))
                              inputs-from-ext (keep (fn [{:keys [witan/input-src-key
                                                                 witan/input-dest-key
                                                                 witan/input-src-fn]}]
                                                      (when input-src-fn
                                                        input-dest-key)) (:witan/inputs ce))
                              diff (clojure.set/difference (set inputs) (set (concat inputs-from-ext outputs-from-parents)))]
                          (when (not-empty diff)
                            (hash-map k diff)))))
                nodes)]
      (when (not-empty missing-inputs-from-parents)
        (throw
         (IllegalArgumentException.
          (str "The following nodes will not receive the required inputs from their parent nodes: " missing-inputs-from-parents)))))

    ;;
    ;; When two or more workflow nodes converge to provide outputs, will there be a clash of keys? (consider output mappings)
    ;;
    (let [clashing-outputs
          (into []
                (comp
                 (filter (fn [[k node]]
                           (> (count (:from node)) 1)))
                 (keep   (fn [[k {:keys [from]}]]
                           (let [parent-outputs (outputs-from-parents-fn from)]
                             (when-not (= (count (set parent-outputs)) (count parent-outputs))
                               (hash-map k parent-outputs))))))
                nodes)]
      (when (not-empty clashing-outputs)
        (throw
         (IllegalArgumentException.
          (str "The following nodes will experience a clash of inputs from their parents (consider using output mappings): " clashing-outputs)))))))

(defn leaf?
  [d]
  (when-not (:pred? d)
    (not (contains? d :to))))

(defn root?
  [d]
  (when-not (:pred? d)
    (let [diff (clojure.set/difference (set (:from d)) (set (:to d)))]
      (empty? diff))))

(defn- validate-params
  [contract catalog-entry]
  (let [params (:witan/params catalog-entry)
        schema (:witan/params-schema contract)
        param-errors (when schema (s/check schema params))]
    (if param-errors
      (throw (Exception. (str "One more more parameters failed to validate: " (clojure.string/trim (prn-str param-errors)) ". Expected: " schema)))
      params)))

(defn- upsert-data
  [node-key {:keys [witan/inputs]} state {:keys [witan/input-src-fn witan/input-src-key witan/input-dest-key]
                                          :or   {witan/input-dest-key input-src-key}
                                          :as   input}]
  (if-not input-src-fn
    (let [existing-data (get state input-src-key)
          existing-schema (when existing-data (some #(when (= (:witan/key %) input-dest-key) (:witan/schema %)) inputs))]
      (if (and existing-schema (not (s/check existing-schema existing-data)))
        (assoc state input-dest-key existing-data)
        (throw (Exception. (format "Input could not be found or validated for %s - %s" node-key input)))))
    (let [fnc (resolve input-src-fn)]
      (if fnc
        (assoc state input-dest-key (fnc input-src-key))
        (throw (Exception. (str input-src-fn " could not be resolved.")))))))

(defn- collect-dependencies
  [node-key inputs contract catalog-entry]
  (let [dependencies (reduce (partial upsert-data node-key contract) inputs (:witan/inputs catalog-entry))
        dependency-errors (->> (:witan/inputs contract)
                               (reduce (fn [a {:keys [witan/schema witan/key]}]
                                         (let [error (s/check schema (get dependencies key))]
                                           (if error (assoc a key error) a))) {}))]
    (if (not-empty dependency-errors)
      (throw (Exception. (format "Failed to procure appropriate inputs for %s: - %s" node-key dependency-errors)))
      dependencies)))

(defn- process-node
  [dependencies params contract]
  (let [fnc (resolve (:witan/impl contract))]
    (if-not fnc
      (throw (Exception. (str "Function failed to resolve:" (:witan/impl contract))))
      (fnc dependencies params))))

(defn- check-outputs
  [result contract]
  (let [result-errors (->> result
                           (reduce (fn [a [k v]]
                                     (let [output (some #(when (= (:witan/key %) k) %) (:witan/outputs contract))]
                                       (assoc a k (if-not output
                                                    "Output is not defined"
                                                    (s/check (:witan/schema output) v))))) {})
                           (remove (comp nil? second))
                           (into {}))
        missing-outputs (clojure.set/difference (->> contract
                                                     :witan/outputs
                                                     (map :witan/key)
                                                     (set)) (keys result))
        result-errors (->> missing-outputs
                           (reduce (fn [a c] (assoc a c "Output is missing")) result-errors)
                           (into {})
                           (not-empty))]
    (if result-errors
      (throw (Exception. (str "One or more outputs failed to validate: " result-errors)))
      result)))

(defn- map-outputs
  [result {:keys [witan/outputs]}]
  (if-let [outputs (not-empty outputs)]
    (reduce (fn [m {:keys [witan/output-src-key witan/output-dest-key]}]
              (-> m
                  (assoc output-dest-key (get m output-src-key))
                  (dissoc output-src-key))) result outputs)
    result))

(defn- create-future
  [{:keys [catalog contracts] :as workspace} node-key {:keys [inputs outputs]}]
  (let [contract (get-contract workspace node-key)
        catalog-entry (get-catalog-entry catalog node-key)]
    (-> (apply d/zip (vals inputs))
        (d/chain
         #(d/future
            (println "IN FUTURE" node-key)
            (let [internal     (->> (disj (set %) :_start) (reduce conj {}))
                  params       (validate-params contract catalog-entry)
                  result       (-> node-key
                                   (collect-dependencies internal contract catalog-entry)
                                   (process-node params contract)
                                   (check-outputs contract)
                                   (map-outputs catalog-entry))]
              (if (not-empty outputs)
                (doseq [output (vals outputs)]
                  (d/success! output result))
                result))))
        (d/catch Exception #(let [ex %]
                              (throw ex))))))

(defn- add-inputs
  [[k v]]
  (vector k (assoc v :inputs (zipmap (:from v) (repeatedly (-> v :from count) d/deferred)))))

(defn- add-outputs
  [[k v] inputs]
  (vector k (assoc v :outputs (into {} (map (fn [x] (vector x (get-in inputs [x :inputs k]))) (:to v))))))

(s/defn execute*
  [{:keys [workflow] :as workspace} :- as/Workspace]
  (validate-workspace workspace)
  (let [nodes        (->> workflow
                          (workflow->long-hand-workflow)
                          (reduce-kv (fn [m k v] (assoc m k (assoc v :inputs (if (root? v) {:_root (d/deferred)} nil)))) {}))
        root-nodes   (not-empty (reduce-kv (fn [m k v] (if (root? v) (conj m k) m)) [] nodes))]
    (if-not root-nodes
      (throw (Exception. "Can't start without any root nodes."))
      (let [with-inputs  (walk/prewalk
                          (fn [x] (cond-> x
                                    (and (vector? x) (-> x second map?) (contains? (-> x second) :from))
                                    (add-inputs))) nodes)
            with-outputs (walk/prewalk
                          (fn [x] (cond-> x
                                    (and (vector? x) (-> x second map?) (contains? (-> x second) :to))
                                    (add-outputs with-inputs))) with-inputs)
            with-futures (reduce-kv (fn [m k v] (assoc-in m [k :future] (create-future workspace k v))) with-outputs with-outputs)]
        ;; kick off
        (println "Running..." with-futures)
        (run! #(d/success! (-> with-futures  % :inputs :_root) :_start) root-nodes)
        ;; wait for finished futures
        (-> (apply d/zip (map (fn [[k v]] (:future v)) with-futures))
            (d/chain)
            (d/catch Exception #(let [ex %]
                                  (log/error ex)
                                  (st/print-stack-trace ex)
                                  (throw ex)))
            (deref))
        ;; gather results
        (->> with-futures
             (reduce-kv (fn [m k v] (if (leaf? v) (conj m (-> v :future deref)) m)) {}))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-channel-map
  [{:keys [to from] :as m}]
  (-> m
      (assoc :inbound  (into {} (map #(hash-map % (async/chan)) from)))
      (assoc :outbound (into {} (map #(hash-map % (async/chan)) to)))))

(defn create-routing
  [node-key {:keys [inbound outbound] :as m}]
  (-> m
      (assoc :runnable )))

(s/defn execute
  [{:keys [workflow] :as workspace} :- as/Workspace]
  #_(validate-workspace workspace)
  (let [nodes (workflow->long-hand-workflow workflow)
        root-nodes    (not-empty (reduce-kv (fn [m k v] (if (root? v) (conj m k) m)) [] nodes))
        with-channels (reduce (fn [a kv] (update a (first kv) create-channel-map)) nodes nodes)
        routers       (map create-routing with-channels)]
    with-channels))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(s/defn workflow->graphviz
  [wf :- as/Workflow]
  (->> wf
       (workflow->long-hand-workflow)
       (map (fn [[k v]] [k (:to v)]))
       (reduce (fn [a [from to]] (update a from concat to)) {})))

(s/defn view-workflow
  [wf :- as/Workflow]
  (let [g (workflow->graphviz wf)]
    (viz/view-graph (keys g) g :node->descriptor (fn [n] {:label n}))))
