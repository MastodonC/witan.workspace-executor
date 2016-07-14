(ns witan.workspace-executor.core
  (:require [manifold.deferred :as d]
            [schema.core :as s]
            [clojure.core.async :as async]
            [clojure.stacktrace :as st]
            [clojure.walk :as walk]
            [com.rpl.specter :as spec
             :refer [select* select-one* selected? filterer view collect comp-paths keypath
                     END ALL LAST FIRST VAL BEGINNING STOP]]
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
                                              (assoc-in [pred :pred?] true)
                                              (assoc-in [loop :target-of] pred))) with-to))
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn egress-node
  [node]
  (and
   (empty? (:to node))
   (:from node)))

(defn ingress-node
  [node]
  (and
   (:to node)
   (empty? (:from node))))

(defn chan-pair
  []
  (let [c (async/chan 10)]
    {:chan c
     :mult (async/mult c)}))

(defn create-channel-map
  [node]
  (-> node
      (assoc :inbound  (async/chan))
      (assoc :outbound (if (egress-node node)
                         {:chan (async/chan)}
                         (chan-pair)))))

(defn kw->fn [kw]
  (try
    (let [user-ns (symbol (namespace kw))
          user-fn (symbol (name kw))]
      (or (ns-resolve user-ns user-fn)
          (throw (Exception.))))
    (catch Throwable e
      (throw (ex-info (str "Could not resolve symbol on the classpath, did you require the file that contains the symbol " kw "?") {:kw kw})))))

(defn catalog-function
  [{:keys [catalog contracts]} label]
  (let [fn-label (select-keys
                  (select-one*
                   [(filterer :witan/name #(= label %)) ALL]
                   catalog)
                  [:witan/fn :witan/version :witan/params])
        func (select-one*
              [(filterer #(= (:witan/fn %)
                             (:witan/fn fn-label))
                         #(= (:witan/version %)
                             (:witan/version fn-label)))
               ALL
               :witan/impl]
              contracts)]
    #((kw->fn func) % (:witan/params fn-label))))

(defn linear-route
  [with-channels from to]
  {;;:from (get-in with-channels [from :outbound :mult])
   ;;:to   (get-in with-channels [to :inbound])
   :pipe (async/tap (get-in with-channels [from :outbound :mult])
                    (get-in with-channels [to :inbound]))})

(defn val-key
  [m t]
  (ffirst (filter (fn [[k v]]
                    (= v t)) m)))

(def vals-set (comp set vals))

(defn merge-route
  [with-channels to froms]
  (let [from-chs (reduce #(assoc %1 %2 (async/chan)) {} froms)
        _ (doseq [[f ch] from-chs]
            (async/tap (get-in with-channels [f :outbound :mult]) ch))
        merged-results (async/chan)]
    (async/pipe merged-results (get-in with-channels [to :inbound]))
    (async/go-loop [remaining-chs (vals-set from-chs)
                    acc {}]
      (let [_ (prn "Waiting for " (map (partial val-key from-chs) remaining-chs))
            [v port] (async/alts! (vec remaining-chs))
            _ (prn "Merging" v "into" acc)
            acc' (merge acc v)
            remaining-chs' (disj remaining-chs port)]
        (if (seq remaining-chs')
          (recur remaining-chs' acc')
          (do
            (async/>! merged-results acc')
            (recur (vals-set from-chs) {})))))))

(defn gather-replay-nodes
  [with-channels loop-target pred-ky]
  (let [visited (atom [])]
    (letfn [(descend [from target]
              (swap! visited conj from)
              (let [target-of-loop (get-in with-channels [target :target-of])
                    froms 
                    (remove #{from}
                            (remove
                             (hash-set target-of-loop)
                             (get-in with-channels [target :from])))
                    _ (prn "F: " from " T: " target " FS: " froms " TOF: " target-of-loop " V: " @visited)]
                (concat froms
                        (if (and target-of-loop (not= target-of-loop pred-ky))
                          (mapcat (partial descend
                                           target-of-loop)
                                  (get-in with-channels [target-of-loop :to]))
                          (mapcat (partial descend target)
                                  (remove #{pred-ky}
                                          (get-in with-channels [target :to])))))))]
      (let [r (mapcat (partial descend loop-target) 
                      (remove #{pred-ky}
                              (get-in with-channels [loop-target :to])))]
        (remove (set @visited) r)))))

(defn predicate-replay
  [workspace with-channels node pred-ky pred-fn]
  (let [loop-target (second (:to node))
        replay-nodes (gather-replay-nodes with-channels loop-target pred-ky)]
    (fn [value]
      (if (pred-fn value)
        true
        (do
  ;        (replay-stuff)
          false)))))

(defn predicate-route
  [workspace with-channels pred-ky node]
  (let [[pass fail] (async/split (predicate-replay workspace with-channels node pred-ky
                                                   (catalog-function workspace pred-ky))
                                 (get-in with-channels [pred-ky :inbound]))]
    {:pass-pipe (async/pipe pass (get-in with-channels [(first (:to node)) :inbound]))
     :fail-pipe (async/pipe fail (get-in with-channels [(second (:to node)) :inbound]))}))

(defn meld-name
  [f s]
  (keyword (str (name f) "-" (name s))))

(defn channel
  [with-channels c]
  (select*
   [(spec/walker c) c]
   with-channels))

(defn ingress-nodes
  [nodes]
  (map first
       (filter
        (fn [[k v]]
          (ingress-node v))
        nodes)))

(defn egress-nodes
  [nodes]
  (map first
       (filter
        (fn [[k v]]
          (egress-node v))
        nodes)))

(defn remove-from-preds
  [node]
  (remove (hash-set (:target-of node)) (:from node)))

(s/defn build!
  [{:keys [workflow] :as workspace} :- as/Workspace]
  #_(validate-workspace workspace)
  (let [nodes         (workflow->long-hand-workflow workflow)
        ingress       (ingress-nodes nodes)
        egress        (egress-nodes nodes)
        with-channels (reduce (fn [a kv] (update a (first kv) create-channel-map)) nodes nodes)
        _ (prn nodes)
        predicates    (into {} (filter (fn [[k v]] (:pred? v)) with-channels))
        many-to-one   (into {} (filter (fn [[k v]] (>= (count (remove-from-preds v)) 2)) with-channels))
        one-to-many   (into {} (filter (fn [[k v]] (and (< (count (remove-from-preds v)) 2) (not (:pred? v)))) with-channels))
        o2m-routers   (reduce-kv (fn [a k v]
                                   (reduce
                                    (fn [a to]
                                      (assoc a
                                             (meld-name k to)
                                             (linear-route with-channels k to)))
                                    a
                                    (remove (set (keys many-to-one)) (:to v))))
                                 {} with-channels)
        m20-routers   (reduce-kv (fn [a k v]
                                   (assoc a k (merge-route with-channels k (remove-from-preds v))))
                                 {} many-to-one)
        pred-routers (reduce-kv (fn [a k v]
                                  (assoc a k
                                         (predicate-route workspace with-channels k v)))
                                {} predicates)
        invokers      (reduce
                       (fn [a [k v]]
                         (assoc a k
                                (async/pipeline 1
                                                (:chan (:outbound v))
                                                (map (catalog-function workspace k))
                                                (:inbound v))))
                       {} (remove (comp :pred? second) with-channels))]
    {:tasks   with-channels
     :ingress ingress
     :egress  egress}))

(defn run!!
  [{:keys [ingress egress tasks] :as workspace} init-data]
  (doseq [in ingress]
    (let [c (get-in tasks [in :inbound])]
      (async/>!! c init-data)))
  (reduce
   (fn [a e]
     (conj a
           (async/<!! (get-in tasks [e :outbound :chan]))))
   []
   egress))

(defn kill!!
  [{:keys [ingress egress tasks] :as workspace} init-data]
  (doseq [in ingress]
    (let [c (get-in tasks [in :inbound])]
      (async/close! c))))

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
