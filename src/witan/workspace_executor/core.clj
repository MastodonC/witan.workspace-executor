(ns witan.workspace-executor.core
  (:require [schema.core :as s]
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

(defrecord Node [name
                 outbound
                 inbound
                 from
                 to
                 target-of
                 pred?
                 error-ch])

(def one? (partial = 1))
(def <one? (partial < 1))

(defn node-type
  [node]
  (let [from-count (count (:from node))]
    (cond
      (:pred? node) :predicate
      (zero? (count (:to node))) :sink
      (one? from-count) :from-one
      (<one? from-count) :from-many
      (zero? from-count) :source)))

(defn predicate-are-target-of
  [target-of [label _]]
  (= target-of label))

(s/defn workflow->long-hand-workflow
  [wf :- as/Workflow]
  (let [fnc-froms   (fn [k ws]
                      (->> ws
                           (remove (partial predicate-are-target-of ((comp :target-of k) ws)))
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
                                              (assoc-in [loop :target-of] pred)
                                              (assoc-in [break :target-of] pred))) with-to))
        with-leaves (->> (set (flatten wf))
                         (reduce (fn [a c] (if (contains? a c) a (conj a (hash-map c {})))) (into {} with-to-with-preds)))

        with-froms  (reduce (fn [a x] (if-let [froms (fnc-froms (first x) with-leaves)]
                                        (conj a (update-in x [1] assoc :from froms))
                                        (conj a x))) [] with-leaves)]

    (mapv (fn [[k v]] (map->Node (assoc v :name k :type (node-type v)))) with-froms)))

(s/defn validate-workspace
  [{:keys [workflow contracts catalog] :as workspace} :- as/Workspace]
  (let [workflow* (set (flatten workflow))
        nodes (workflow->long-hand-workflow workflow)
        outputs-from-parents-fn
        (fn [from]
          (mapcat (fn [x]
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

(defn chan-pair
  []
  (let [c (async/chan 10)]
    {:chan c
     :mult (async/mult c)}))

(defn =node-type
  [nt]
  #(= nt (:type %)))

(defn create-channel-map
  [error-ch node]
  (-> node
      (assoc :inbound  (async/chan))
      (assoc :outbound (if ((=node-type :sink) node)
                         {:chan (async/chan)}
                         (chan-pair)))
      (assoc :error-ch error-ch)))

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

(defn meld-name
  [f s]
  (keyword (str (name f) "-" (name s))))

(defn meld-merge-name
  [node]
  (keyword (str "merge-of-" (apply str (interpose "-and-" (map name (:from node)))) "-to-" (name (:name node)))))

(defprotocol IActivatable
  (activate! [this]))

(defprotocol IRouter
  (replay! [this])
  (responsible? [this node-name])
  (label [this])
  (state [this]))

(deftype LinearRouter [label ^Node from ^Node to state]
  IActivatable
  (activate!  [this]
    (async/tap (get-in from [:outbound :mult])
               (get-in to [:inbound]))
    (let [storage-chan (async/chan)]
      (async/tap (get-in from [:outbound :mult])
                 storage-chan)
      (async/go-loop [store (async/<! storage-chan)]
        (reset! state store)
        (recur (async/<! storage-chan))))
    this)
  IRouter  
  (replay! [this]
    (async/>!! (get-in to [:inbound]) @state))
  (state [this]
    @state)
  (label [this]
    label)
  (responsible? [this node-name]
    (= node-name (:name to))))

(defn port->node
  [port nodes]
  (some #(when (= port (:inbound %)) %) nodes))

(deftype MergeRouter [label from-routers merge-nodes ^Node to]
  IActivatable
  (activate!  [this]
    (async/go-loop [remaining-chs (set merge-nodes)
                    acc {}]
      (let [[v port] (async/alts! (mapv :inbound remaining-chs))
            node (port->node port merge-nodes)              
            acc' (merge acc v)
            remaining-chs' (disj remaining-chs node)]
        (if (seq remaining-chs')
          (recur remaining-chs' acc')
          (do
            ;;(prn "Outputting to" to)
            (async/>! (:inbound to) acc')
            (recur (set merge-nodes) {}))))))
  IRouter
  (replay! [this])
  (state [this])
  (label [this]
    label)
  (responsible? [this node-name]))

(defn gather-replay-edges
  [with-channels node]
  (let [loop-target (second (:to node))
        pred-ky (:name node)
        visited (atom [])]
    (letfn [(descend [from target]
              (swap! visited conj from target)
              (let [target-of-loop (get-in with-channels [target :target-of])
                    froms (remove #{from}
                                  (get-in with-channels [target :from]))]
                (concat (mapv #(vector % target) froms)
                        (if (and target-of-loop (not= target-of-loop pred-ky))
                          (mapcat (partial descend target-of-loop)
                                  (remove (set @visited)
                                          (get-in with-channels [target-of-loop :to])))
                          (mapcat (partial descend target)
                                  (remove (conj (set @visited) pred-ky)
                                          (get-in with-channels [target :to])))))))]
      (let [r (mapcat (partial descend loop-target)
                      (remove #{pred-ky}
                              (get-in with-channels [loop-target :to])))]
        (remove #((set @visited) (first %)) r)))))

(defn edge->router
  [routers edge]
  (get routers
       (apply meld-name edge)))

(defn predicate-replay
  [replay-routers]
  (fn [value]
    (if (first value)
      true
      (do
        (doseq [router replay-routers]
          (.replay! router))
        false))))

(deftype PredicateRouter [label tos pred-node pred-replay-fn replay-routers]
  IActivatable
  (activate!  [this]
    (let [tapped-pred-out (async/chan)     
          [pass fail] (async/split pred-replay-fn
                                   tapped-pred-out)]
      (async/tap (get-in pred-node [:outbound :mult])
                 tapped-pred-out)
      (async/pipeline 1 (:inbound (first tos)) (map second) pass)
      (async/pipeline 1 (:inbound (second tos)) (map second) fail)))
  IRouter
  (replay! [this]
    (doseq [r replay-routers]
      (.replay! r)))
  (state [this])
  (label [this]
    label)
  (responsible? [this node-name]
    (= node-name (:name pred-node))))

(defprotocol IInvoker
  (kill! [this]))

(deftype Invoker [node kill func]
  IActivatable
  (activate! [this]
    (let [in       (:inbound node)
          out      (get-in node [:outbound :chan])
          error-ch (:error-ch node)]
      (async/go-loop [[val port] (async/alts! [in kill])]
        (when (= port in)
          (try
            (async/>! out (func val))
            (catch Exception e
              (async/>! error-ch e)))
          (recur (async/alts! [in kill]))))))
  IInvoker
  (kill! [this]
    (async/close! kill)))

(deftype PredicateInvoker [node kill pred]
  IActivatable
  (activate! [this]
    (let [in       (:inbound node)
          out      (get-in node [:outbound :chan])
          error-ch (:error-ch node)]
      (async/go-loop [[val port] (async/alts! [in kill])]
        (when (= port in)
          (try
            (async/>! out [(pred val) val])
            (catch Exception e
              (async/>! error-ch e)))
          (recur (async/alts! [in kill]))))))
  IInvoker
  (kill! [this]
    (async/close! kill)))

(defmulti create-router
  "Returns a vector router connecting node to :from nodes"
  (fn [name->node node] (:type node)))

(defmethod create-router :from-one
  [name->node node]
  (let [from-node ((first (:from node)) name->node)]
    (LinearRouter.
     (meld-name (:name from-node) (:name node))
     from-node
     node
     (atom nil))))

(defmethod create-router :sink
  [name->node node]
  (when-let [from-node (get name->node (first (:from node [])))]
    (LinearRouter.
     (meld-name (:name from-node) (:name node))
     from-node
     node
     (atom nil))))

(defmethod create-router :from-many
  [name->node node]
  (let [from-nodes (map 
                    #(get name->node %)
                    (:from node))
        merge-nodes (map #(map->Node
                     {:inbound (async/chan)
                      :name (keyword (str (name (:name %)) "_merge"))})
                   from-nodes)
        linear-routers (map #(LinearRouter.
                              (meld-name (:name %1) (:name node)) %1 %2 (atom nil))
                            from-nodes
                            merge-nodes)]
    (cons (MergeRouter. (meld-merge-name node) linear-routers merge-nodes node)
          linear-routers)))

(defn create-predicate-router
  [name->node edge-name->router node]
  (let [replay-routers (map (partial edge->router edge-name->router) 
                            (gather-replay-edges name->node node))
        pred-replay-fn (predicate-replay replay-routers)
        from-node (get name->node (first (:from node)))]
    [(LinearRouter.
      (meld-name (:name from-node) (:name node))
      from-node
      node
      (atom nil))
     (PredicateRouter.
      (:name node)
      (map (partial get name->node) (:to node))
      node
      pred-replay-fn
      replay-routers)]))

(defmulti create-invoker 
  (fn [^Node node func] (:type node)))

(defmethod create-invoker :predicate
  [^Node node func]
  (PredicateInvoker.
   node
   (async/chan)
   func))

(defmethod create-invoker :default
  [^Node node func]
  (Invoker.
   node
   (async/chan)
   func))

(defn reduce-nodes-to-map-name->node
  [nodes]
  (reduce (fn [a n] (assoc a (:name n) n)) {} nodes))

(def map-flat (comp flatten keep))

(defn activate-all!
  [activatables]
  (doseq [a activatables]
    (activate! a)))

(s/defn build!
  [{:keys [workflow] :as workspace} :- as/Workspace]
  #_(validate-workspace workspace)
  (let [error-channel (async/chan)
        nodes         (mapv (partial create-channel-map error-channel) 
                            (workflow->long-hand-workflow workflow))
        ingress       (filter (=node-type :source) nodes)
        egress        (filter (=node-type :sink) nodes)
        name->node    (reduce-nodes-to-map-name->node nodes)
        
        routers (map-flat (partial create-router name->node) 
                          (remove (=node-type :source) 
                                  (remove (=node-type :predicate)
                                          nodes)))

        edge-name->router (reduce (fn [a r] (assoc a (label r) r)) {} routers)
        predicate-routers (mapcat 
                           (partial create-predicate-router name->node edge-name->router)
                           (filter (=node-type :predicate) nodes))

        invokers      (map
                       (fn [n]
                         (create-invoker n (catalog-function workspace (:name n))))
                       nodes)

        _ (activate-all! (concat predicate-routers routers invokers))]
    {:tasks    name->node
     :ingress  ingress
     :egress   egress
     :routers  (reduce 
                #(assoc %1 (label %2) %2) edge-name->router predicate-routers)
     :invokers (reduce #(assoc %1 (:name (.node %2)) %2) {} invokers)
     :error-ch error-channel}))

(defn await-results
  [{:keys [egress tasks error-ch] :as workspace-network}]
  (loop [remaining-chs (set (map #(get-in % [:outbound :chan]) egress))
         acc-results []]
    (if (seq remaining-chs)
      (let [[v p] (async/alts!! (cons error-ch remaining-chs))]
        (if (= p error-ch)
          (cons {:error v} acc-results)
          (recur (disj remaining-chs p) (cons v acc-results))))
      acc-results))  )

(defn run!!
  [{:keys [ingress egress tasks] :as workspace-network} init-data]
  (doseq [in ingress]
    (let [c (get in :inbound)]
      (async/>!! c init-data)))
  (await-results workspace-network))

(defn update-network!
  [{:keys [invokers tasks] :as workspace-network} workspace nodes-changed]
  (doseq [invoker (map invokers nodes-changed)]
    (kill! invoker))
  (reduce
   (fn [wn node-name]
     (assoc-in wn
                [:invokers node-name]
                (activate! (create-invoker (tasks node-name) (catalog-function workspace node-name)))))
   workspace-network
   nodes-changed))

(defn replay-nodes!
  [{:keys [routers] :as workspace-network} node-names]
  (doseq [r (mapcat (fn [n] (filter (fn [r] (responsible? r n)) (vals routers))) node-names)]
    (replay! r)))

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
