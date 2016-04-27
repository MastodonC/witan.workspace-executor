(ns witan.workspace-executor.onyx-test
  (:require [clojure.test :refer :all]
            [schema.test :as st]
            [witan.workspace-executor.onyx :as o]
            [witan.workspace-executor.function-catalog :as fc]))

(use-fixtures :once st/validate-schemas)

(defn enough?
  [x]
  (< 10 x))

(defn workspace
  [{:keys [workflow contracts catalog] :as raw}]
  (-> 
   raw
   (assoc :workflow (or workflow []))
   (assoc :contracts (or contracts []))
   (assoc :catalog (or catalog []))))

(def config
  {:redis-config {:redis/uri "redis"}
   :batch-settings {:onyx/batch-size 1}})

(defn batch-size
  [c]
  (get-in c [:batch-settings :onyx/batch-size]))

(defn redis-uri
  [c]
  (get-in c [:redis-config :redis/uri]))

(defn all-same
  [coll]
  (= 1 (count (set coll))))

(deftest witan-workflow->onyx-workflow
  (testing "Simple liner workflows should be unchanged"
    (is (= {:workflow [[:in :inc]
                       [:inc :out]]
            :flow-conditions []
            :lifecycles []}
           (o/witan-workflow->onyx-workflow
            {:workflow [[:in :inc]
                        [:inc :out]]}
            {}))))

  (testing "Loops should be introduced when branches are used"
    (let [onyx-job (o/witan-workflow->onyx-workflow
                    {:workflow [[:in :inc]
                                [:inc [:enough? :out :inc]]]
                     :catalog []
                     :task-scheduler :onyx.task-scheduler/balanced}
                    config)]
      (is (= [[:in :write-state-inc]
              [:inc :write-state-inc]
              [:read-state-inc :inc]
              [:inc :out]]
             (:workflow onyx-job)))
      (is (= [{:flow/from :inc,
               :flow/to [:write-state-inc],
               :flow/predicate [:not :enough?]}
              {:flow/from :inc, 
               :flow/to [:out], :flow/predicate 
               [:enough?]}]
             (:flow-conditions onyx-job)))
      (is (= [{:lifecycle/task :out
               :lifecycle/calls :onyx.plugin.redis/remove-redis-key
               :redis/uri (redis-uri config)}]
             (mapv #(dissoc % :redis/key) 
                   (:lifecycles onyx-job))))
      (is (= [{:onyx/name :write-state-inc,
               :onyx/plugin :onyx.plugin.redis/writer,
               :onyx/type :output,
               :onyx/medium :redis,
               :redis/uri (redis-uri config)
               :onyx/batch-size (batch-size config)}
              {:onyx/name :read-state-inc,
               :onyx/plugin :onyx.plugin.redis/reader,
               :onyx/type :input,
               :onyx/medium :redis,
               :onyx/max-peers 1
               :redis/uri (redis-uri config)
               :onyx/batch-size (batch-size config)}]
             (mapv #(dissoc % :redis/key) (:catalog onyx-job))))
      (let [redis-key-entries (concat
                               (map :redis/key (:catalog onyx-job))
                               (map :redis/key (:lifecycles onyx-job)))]
        (is (every?
             #(.startsWith (name %) "state-inc-")
             redis-key-entries))
        (is (all-same redis-key-entries))))))

(deftest witan-catalog->onyx-catalog
  (testing "Simple function gets relabeled"
    (is (= {:catalog
            [{:onyx/name :inc
              :onyx/fn   :witan.workspace-executor.function-catalog/my-inc
              :onyx/type :function
              :onyx/batch-size (batch-size config)}]}
           (o/witan-catalog->onyx-catalog
            {:catalog
             (filter #(= :inc (:witan/name %)) 
                     fc/catalog)}
            config)))))


(deftest witan-workspace->onyx-job
  (is (= {:workflow [[:in :inc]                     
                     [:inc :out]]
                                        ;   :contracts []
          :catalog [{:onyx/name :inc
                     :onyx/fn   :witan.workspace-executor.function-catalog/my-inc
                     :onyx/type :function
                     :onyx/batch-size (batch-size config)}]
          :flow-conditions []
          :lifecycles []
          :task-scheduler :onyx.task-scheduler/balanced}
         (o/workspace->onyx-job
          (workspace
           {:workflow [[:in :inc]
                       [:inc :out]]
            :catalog [{:witan/name :inc
                       :witan/fn :witan.workspace-executor.function-catalog/my-inc
                       :witan/version "1.0"
                       :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                       :witan/input-src-key  1
                                       :witan/input-dest-key :number}]}]})
          config))))
