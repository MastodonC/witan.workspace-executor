(ns witan.workspace-executor.core
  (:require [manifold.deferred :as d]
            [schema.core :as s]
            [clojure.core.async :as async]
            [clojure.stacktrace :as st]
            [clojure.walk :as walk]
            [rhizome.viz :as viz]
            [taoensso.timbre :as log]
            ;;
            [witan.workspace-executor.schema :as as]))

(defn- validate-workspace
  [{:keys [workflow contracts catalog]}]
  (let [workflow* (set workflow)]
    (let [catalog-names (->> catalog
                             (map :witan/name)
                             (set))
          diff (not-empty (clojure.set/difference workflow* catalog-names))]
      (when diff
        (throw (IllegalArgumentException. (str "One or more workflow entries are unrepresented in the catalog: " diff)))))
    (let [contract-miss (->> workflow*
                             (keep (fn [a] (some #(when (= (:witan/name %) a) ((juxt :witan/fn :witan/version) %)) catalog)))
                             (keep (fn [[fn-name fn-version]]
                                     (some #(when-not (or (= (:witan/fn %) fn-name)
                                                          (= (:witan/version %) fn-version)) [fn-name fn-version]) contracts)))
                             (vec))]
      (when (not-empty contract-miss)
        (throw (IllegalArgumentException. (str "There are no contracts for the following function + version combinations " contract-miss)))))))

(def leaf?
  #(not (contains? % :to)))

(def root?
  #(not (contains? % :from)))

(s/defn workflow->long-hand-workflow
  [wf :- as/Workflow]
  (let [fnc-froms   (fn [k ws]
                      (->> ws
                           (filter (fn [y] (some #{k} (-> y (second) :to))))
                           (reduce (fn [a c] (conj a (first c))) [])
                           (not-empty)))
        with-to     (->> wf
                         (partition 2)
                         (reduce (fn [x [from to]] (assoc x from (conj (get x from []) to))) {})
                         (map (fn [[n t]] (hash-map n {:to t}))))
        with-leaves (->> (set wf)
                         (reduce (fn [a c] (if (contains? a c) a (conj a (hash-map c {})))) (into {} with-to)))
        with-froms  (reduce (fn [a x] (if-let [froms (fnc-froms (first x) with-leaves)]
                                        (conj a (update-in x [1] assoc :from froms))
                                        (conj a x))) [] with-leaves)]
    (into (sorted-map) with-froms)))

(defn- get-catalog-entry
  [catalog id]
  (some #(when (= (:witan/name %) id) %) catalog))

(defn- get-contract
  [{:keys [catalog contracts]} id]
  (let [catalog-entry          (get-catalog-entry catalog id)
        fnc                    (:witan/fn catalog-entry)
        version                (:witan/version catalog-entry)]
    (some #(when (and (= (:witan/fn %) fnc) (= (:witan/version %) version)) %) contracts)))

(defn- validate-params
  [contract catalog-entry]
  (let [params (:witan/params catalog-entry)
        schema (:witan/params-schema contract)
        param-errors (when schema (s/check schema params))]
    (if param-errors
      (throw (Exception. (str "One more more parameters failed to validate: " (clojure.string/trim (prn-str param-errors)) ". Expected: " schema)))
      params)))

(defn- upsert-data
  [{:keys [witan/inputs]} state {:keys [witan/input-src-fn witan/input-src-key witan/input-dest-key]
                                 :or   {witan/input-dest-key input-src-key}
                                 :as   input}]
  (if-not input-src-fn
    (let [existing-data (get state input-src-key)
          existing-schema (when existing-data (some #(when (= (:witan/key %) input-dest-key) (:witan/schema %)) inputs))]
      (if (and existing-schema (not (s/check existing-schema existing-data)))
        (assoc state input-dest-key existing-data)
        (throw (Exception. (str "Input could not be found or validated: " input)))))
    (let [fnc (resolve input-src-fn)]
      (if fnc
        (assoc state input-dest-key (fnc input-src-key))
        (throw (Exception. (str input-src-fn " could not be resolved.")))))))

(defn- collect-dependencies
  [node-key inputs contract catalog-entry]
  (let [dependencies (reduce (partial upsert-data contract) inputs (:witan/inputs catalog-entry))
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

(defn- create-future
  [{:keys [catalog contracts] :as workspace} node-key {:keys [inputs outputs]}]
  (let [contract (get-contract workspace node-key)
        catalog-entry (get-catalog-entry catalog node-key)]
    ;; TODO - add checks to see if inputs and outputs are VALID.
    (-> (apply d/zip (vals inputs))
        (d/chain
         #(d/future
            (let [internal     (->> (disj (set %) :_start) (reduce conj {}))
                  params       (validate-params contract catalog-entry)
                  dependencies (collect-dependencies node-key internal contract catalog-entry)
                  result       (process-node dependencies params contract)
                  result       (check-outputs result contract)]
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

(s/defn execute
  [{:keys [workflow] :as workspace} :- as/Workspace]
  (validate-workspace workspace)
  (let [nodes        (->> workflow
                          (workflow->long-hand-workflow)
                          (reduce-kv (fn [m k v] (assoc m k (assoc v :inputs (if (root? v) {:_root (d/deferred)} nil)))) {}))
        root-nodes   (reduce-kv (fn [m k v] (if (root? v) (conj m k) m)) [] nodes)
        with-inputs  (walk/prewalk
                      (fn [x] (cond-> x
                                (and (vector? x) (-> x second map?) (contains? (-> x second) :from))
                                (add-inputs))) nodes)
        with-outputs (walk/prewalk
                      (fn [x] (cond-> x
                                (and (vector? x) (-> x second map?) (contains? (-> x second) :to))
                                (add-outputs with-inputs))) with-inputs)
        with-futures (reduce-kv (fn [m k v] (assoc-in m [k :future] (create-future workspace k v))) with-outputs with-outputs)]
    ;; kick off
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
         (reduce-kv (fn [m k v] (if (leaf? v) (conj m (hash-map k (-> v :future deref))) m)) {}))))


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
