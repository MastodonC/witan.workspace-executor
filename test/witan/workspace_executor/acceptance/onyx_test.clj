(ns witan.workspace-executor.acceptance.onyx-test
  (:require [clojure.test :refer :all]
            [schema.test :as st]
            [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe <!! put! close!]]
            [clojure.core.async.lab :refer [spool]]
            [witan.workspace-executor.onyx :as o]
            [witan.workspace-executor.function-catalog :as fc]
            [onyx
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin
             [redis]
             [core-async :refer [get-core-async-channels]]]
            [onyx.tasks
             [core-async :as core-async]
             [redis :as redis]]
            [taoensso.carmine :as car :refer [wcar]]))

(def config (atom {}))

(def batch-settings
  {:batch-settings {:onyx/batch-size 1
                    :onyx/batch-timeout 1000}})

(defn redis-conn []
  {:spec {:uri (get-in @config [:redis-config :redis/uri])}})

(defn load-config [test-fn]
  (reset! config (merge (read-config (clojure.java.io/resource "config.edn") {:profile :test})
                        batch-settings))
  (test-fn))

(use-fixtures :each load-config)

(defn add-source-and-sink
  [job]
  (-> job
      (add-task (core-async/input :in (:batch-settings @config)))
      (add-task (core-async/output :out (:batch-settings @config)))))

(defn run-job
  [job data]
  (let [{:keys [env-config
                peer-config]} @config
        redis-spec (redis-conn)
        {:keys [out in]} (get-core-async-channels job)]
    (with-test-env [test-env [7 env-config peer-config]]
      (pipe (spool [data :done]) in)
      (onyx.test-helper/validate-enough-peers! test-env job)
      (let [job-id (:job-id (onyx.api/submit-job peer-config job))
            result (<!! out)]
        (onyx.api/await-job-completion peer-config job-id)
        result))))

(deftest linear-workspace-executed-on-onyx
  (let [state {:test "blah" :number 0}]
    (is (= (fc/my-inc state)
           (run-job
            (add-source-and-sink
             (o/workspace->onyx-job
              {:workflow [[:in :inc]
                          [:inc :out]]
               :catalog [{:witan/name :inc
                          :witan/fn :witan.workspace-executor.function-catalog/my-inc}]}
              @config))
            state)))))

(deftest looping-workspace-executed-on-onyx
  (let [state {:test "blah" :number 0}]
    (is (= (nth (iterate fc/my-inc state) 10)
           (run-job
            (add-source-and-sink
             (o/workspace->onyx-job
              {:workflow [[:in :inc]
                          [:inc [:witan.workspace-executor.function-catalog/gte-ten :out :inc]]]
               :catalog [{:witan/name :inc
                          :witan/fn :witan.workspace-executor.function-catalog/my-inc}]}
              @config))
            state)))))

(deftest merge-workspace-executed-on-onyx
  (let [state {:test "blah" :number 0}]
    (is (= (fc/sum (merge (fc/mult state) (fc/my-inc state)))
           (run-job
            (add-source-and-sink
             (o/workspace->onyx-job
              {:workflow [[:in :inc]
                          [:in :mult]
                          [:inc :sum]
                          [:mult :sum]
                          [:sum :out]]
               :catalog [{:witan/name :inc
                          :witan/fn :witan.workspace-executor.function-catalog/my-inc}
                         {:witan/name :mult
                          :witan/fn :witan.workspace-executor.function-catalog/mult}
                         {:witan/name :sum
                          :witan/fn :witan.workspace-executor.function-catalog/sum}]
               :task-scheduler :onyx.task-scheduler/balanced}
              @config))
            state)))))


(comment [:get-births-data-year :at-risk-this-birth-year
               :get-births-data-year :at-risk-last-birth-year
               :at-risk-this-birth-year :births-pool
               :at-risk-last-birth-year :births-pool
               ;; :births-pool :births
               ;; :births :fert-rate-without-45-49
               ;; :fert-rate-without-45-49 :fert-rate-with-45-49
               ;; :at-risk-this-fert-last-year :estimated-sya-births-pool
               ;; :at-risk-last-fert-last-year :estimated-sya-births-pool
               ;; :fert-rate-with-45-49 :estimated-sya-births
               ;; :estimated-sya-births-pool :estimated-sya-births
               ;; :estimated-sya-births :estimated-births
               ;; :estimated-sya-births :historic-fertility
               ;; :estimated-births :scaling-factors
               ;; :actual-births :scaling-factors
               ;; :scaling-factors :historic-fertility
])
