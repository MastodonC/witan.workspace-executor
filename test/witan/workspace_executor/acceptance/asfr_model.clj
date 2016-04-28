(ns witan.workspace-executor.acceptance.asfr-model
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


(def witan-workflow [[:in :get-births-data-year]
                     [:in :at-risk-this-fert-last-year]
                     [:in :at-risk-last-fert-last-year]
                     [:in :actual-births]
                     [:get-births-data-year :at-risk-this-birth-year]
                     [:get-births-data-year :at-risk-last-birth-year]
                     [:at-risk-this-birth-year :births-pool]
                     [:at-risk-last-birth-year :births-pool]
                     [:births-pool :births]
                     [:births :fert-rate-without-45-49]
                     [:fert-rate-without-45-49 :fert-rate-with-45-49]
                     [:at-risk-this-fert-last-year :estimated-sya-births-pool]
                     [:at-risk-last-fert-last-year :estimated-sya-births-pool]
                     [:fert-rate-with-45-49 :estimated-sya-births]
                     [:estimated-sya-births-pool :estimated-sya-births]
                     [:estimated-sya-births :estimated-births]
                     [:estimated-sya-births :historic-fertility]
                     [:estimated-births :scaling-factors]
                     [:actual-births :scaling-factors]
                     [:scaling-factors :historic-fertility]
                     [:historic-fertility :out]])

(def all-functions (set witan-workflow))

(defn create-function
  [kw]
  (let [b (symbol "bundle")]
    `(defn ~(symbol (namespace kw) (name kw))
       [~b]
       (assoc ~b ~kw true))))

(defn births [bundle] (assoc bundle :births true)) 
(defn get-births-data-year [bundle] (assoc bundle :get-births-data-year true))
(defn births-pool [bundle] (assoc bundle :births-pool true))
(defn estimated-sya-births-pool [bundle] (assoc bundle :estimated-sya-births-pool true))
(defn at-risk-this-fert-last-year [bundle] (assoc bundle :at-risk-this-fert-last-year true))
(defn fert-rate-without-45-49 [bundle] (assoc bundle :fert-rate-without-45-49 true))
(defn historic-fertility [bundle] (assoc bundle :historic-fertility true))
(defn fert-rate-with-45-49 [bundle] (assoc bundle :fert-rate-with-45-49 true))
(defn estimated-sya-births [bundle] (assoc bundle :estimated-sya-births true))
(defn scaling-factors [bundle] (assoc bundle :scaling-factors true))
(defn actual-births [bundle] (assoc bundle :actual-births true))
(defn at-risk-last-birth-year [bundle] (assoc bundle :at-risk-last-birth-year true))
(defn estimated-births [bundle] (assoc bundle :estimated-births true))
(defn at-risk-last-fert-last-year [bundle] (assoc bundle :at-risk-last-fert-last-year true))
(defn at-risk-this-birth-year [bundle] (assoc bundle :at-risk-this-birth-year true))

(comment "Smash the set of the function names through create-function to get default function defs")


(defn create-catalog
  [kw]
  {:witan/name kw
   :witan/fn (keyword (str "witan.workspace-executor.acceptance.asfr-model/" (name kw)))})



(def catalog 
  [{:witan/fn :witan.workspace-executor.acceptance.asfr-model/births, :witan/name :births}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/get-births-data-year, :witan/name :get-births-data-year}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/births-pool, :witan/name :births-pool}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/estimated-sya-births-pool, :witan/name :estimated-sya-births-pool}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/at-risk-this-fert-last-year, :witan/name :at-risk-this-fert-last-year}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/fert-rate-without-45-49, :witan/name :fert-rate-without-45-49}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/historic-fertility, :witan/name :historic-fertility}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/fert-rate-with-45-49, :witan/name :fert-rate-with-45-49}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/estimated-sya-births, :witan/name :estimated-sya-births}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/scaling-factors, :witan/name :scaling-factors}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/actual-births, :witan/name :actual-births}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/at-risk-last-birth-year, :witan/name :at-risk-last-birth-year}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/estimated-births, :witan/name :estimated-births}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/at-risk-last-fert-last-year, :witan/name :at-risk-last-fert-last-year}
   {:witan/fn :witan.workspace-executor.acceptance.asfr-model/at-risk-this-birth-year, :witan/name :at-risk-this-birth-year}])


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
    (with-test-env [test-env [30 env-config peer-config]]
      (pipe (spool [data :done]) in)
      (onyx.test-helper/validate-enough-peers! test-env job)
      (let [job-id (:job-id (onyx.api/submit-job peer-config job))
            result (<!! out)]
        (onyx.api/await-job-completion peer-config job-id)
        result))))

(def result
  {:births true
   :get-births-data-year true
   :births-pool true
   :estimated-sya-births-pool true
   :at-risk-this-fert-last-year true
   :fert-rate-without-45-49 true
   :historic-fertility true
   :fert-rate-with-45-49 true
   :estimated-sya-births true
   :scaling-factors true
   :actual-births true
   :at-risk-last-birth-year true
   :estimated-births true
   :at-risk-last-fert-last-year true
   :at-risk-this-birth-year true})

(deftest crazy-workflow-works
  (let [state {}]
    (is (= result
           (run-job
            (add-source-and-sink
             (o/workspace->onyx-job
              {:workflow witan-workflow
               :catalog catalog}
              @config))
            state)))))
