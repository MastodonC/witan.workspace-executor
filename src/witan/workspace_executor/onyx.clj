(ns witan.workspace-executor.onyx
  (:require [schema.core :as s]
            [witan.workspace-executor.core :as wex]
            [com.rpl.specter :as spec 
             :refer [transform select selected? filterer setval view collect comp-paths keypath
                     END ALL LAST FIRST VAL BEGINNING STOP]]
            [com.rpl.specter.macros :refer [defpath]]
            [onyx.plugin
             [redis :as redis]
             [core-async :refer [get-core-async-channels]]]
            [onyx.tasks
             [core-async :as core-async]
             [redis :as redis-tasks]]
            [witan.workspace-executor.schema :as as]
            [witan.workspace-executor.utils :as utils]
            [onyx.job :as onyx.job]
            [onyx.test-helper :refer [with-test-env]]))


(defn branch?
  [v]
  (vector? (second v)))

(defn third 
  [v] 
  (nth v 2))

(def llast (comp last last))
(def ssecond (comp second second))
(def flast (comp first last))

(defn equals
  [v]
  (fn [c]
    (= v c)))

(defpath value-path [key]
  (select* 
   [this structure next-fn]
   (next-fn (filter (equals key) structure)))
  (transform* 
   [this structure next-fn]
   (if (= structure key)
     (next-fn key)
     structure)))

(def wf-to-task
  (comp-paths :workflow ALL LAST value-path))

(def wf-end
  (comp-paths :workflow END))

(def wf-node
  (comp-paths :workflow ALL value-path))

(def fc-end
  (comp-paths :flow-conditions END))

(def lc-end
  (comp-paths :lifecycles END))

(defn add-task
  [task job]
  (onyx.job/add-task job task))

(defn write-state-kw
  [branch]
  (keyword (str "write-state-" (name (first branch)))))

(defn read-state-kw
  [branch]
  (keyword (str "read-state-" (name (first branch)))))

(defn redis-key-for
  [branch]
  (keyword (str "state-" (name (first branch)) "-" (str (java.util.UUID/randomUUID)))))

(defn flow-condition
  [node pred]
  [{:flow/from (first node)
    :flow/to [(second node)]
    :flow/predicate pred}])

(defn life-cycle
  [task redis-key redis-uri]
  [{:lifecycle/task task
    :lifecycle/calls :onyx.plugin.redis/remove-redis-key
    :redis/key redis-key
    :redis/uri redis-uri}])

(defn branch-expanders
  [config]
  (let [redis-uri (:redis/uri (:redis-config config))
        batch-settings (:batch-settings config)]
    (fn
      [branch]
      (let [write-state (write-state-kw branch)
            read-state (read-state-kw branch)
            redis-key (redis-key-for branch)
            loop-node [(first branch) write-state]
            exit-node [(first branch) (ssecond branch)]
            branch-fn (flast branch)]
        (fn [raw]
          (->> raw
               (transform
                (wf-to-task (llast branch))
                (constantly write-state))
               (transform
                wf-end
                (constantly
                 (vector [read-state (llast branch)])))
               (transform
                (wf-node branch)
                (constantly
                 loop-node))
               (transform
                wf-end
                (constantly
                 (vector exit-node)))
               (setval
                fc-end
                (flow-condition
                 loop-node
                 [:not branch-fn]))
               (setval
                fc-end
                (flow-condition
                 exit-node
                 [branch-fn]))
               (add-task (redis-tasks/writer write-state redis-uri redis-key batch-settings))
               (add-task (redis-tasks/reader read-state redis-uri redis-key batch-settings))
               (setval
                lc-end
                (life-cycle (last exit-node)
                            redis-key
                            redis-uri))))))))

(defn schema-wrapper
  [input-schema output-schema fn-wrapped segment]
  (s/validate input-schema segment)
  (let [output (fn-wrapped segment)]
    (s/validate output-schema output)
    output))

(def onyx-defaults
  {:task-scheduler :onyx.task-scheduler/balanced})

(defn witan-workflow->onyx-workflow
  [{:keys [workflow] :as workspace} config]
  ((->>
    (select [:workflow ALL branch?] workspace)
    (map (branch-expanders config))
    (apply comp identity)) 
   (assoc workspace
          :flow-conditions []
          :lifecycles [])))

(def onyx-catalog-entry-template
  {:onyx/name :witan/name
   :onyx/fn :witan/fn
   :onyx/type (constantly :function)
   :onyx/batch-size (fn [_ config]
                      (get-in config [:batch-settings :onyx/batch-size]))})

(defn witan-catalog->onyx-catalog
  [{:keys [catalog] :as workspace}
   config]
  (update workspace
          :catalog
          #(mapv (fn [cat]
                  (reduce-kv
                   (fn [acc onyx-key getter]
                     (assoc acc
                            onyx-key
                            (getter cat config)))
                   {}
                   onyx-catalog-entry-template))
                 %)))

(s/defn workspace->onyx-job
  [{:keys [workflow] :as workspace} :- as/Workspace
   config]
  (->
   (merge workspace
          onyx-defaults)
   (dissoc :contracts)
   (witan-catalog->onyx-catalog config)
   (witan-workflow->onyx-workflow config)))