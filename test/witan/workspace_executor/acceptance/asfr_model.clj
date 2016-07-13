(ns witan.workspace-executor.acceptance.asfr-model
  (:require [clojure.test :refer :all]
            [schema.test :as st]
            [schema.core :as s]
            [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe <!! put! close!]]
            [clojure.core.async.lab :refer [spool]]
            [witan.workspace-executor.core :as wex]
            [witan.workspace-executor.function-catalog :as fc]))


(def witan-workflow [[:in1 :get-births-data-year]
                     [:in2 :at-risk-this-fert-last-year]
                     [:in3 :at-risk-last-fert-last-year]
                     [:in4 :actual-births]
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

(def all-functions (set (flatten witan-workflow)))

(defn create-function
  [kw]
  (let [b (symbol "bundle")]
    `(defn ~(symbol (namespace kw) (name kw))
       [~b]
       (assoc ~b ~kw true))))

(defn pass-through [_ init] init)
(defn out [msg _] (prn "result" msg) msg)


(defn births [bundle _] (assoc bundle :births true))
(defn get-births-data-year [bundle _] (assoc bundle :get-births-data-year true))
(defn births-pool [bundle _] (assoc bundle :births-pool true))
(defn estimated-sya-births-pool [bundle _] (assoc bundle :estimated-sya-births-pool true))
(defn at-risk-this-fert-last-year [bundle _] (assoc bundle :at-risk-this-fert-last-year true))
(defn fert-rate-without-45-49 [bundle _] (assoc bundle :fert-rate-without-45-49 true))
(defn historic-fertility [bundle _] (assoc bundle :historic-fertility true))
(defn fert-rate-with-45-49 [bundle _] (assoc bundle :fert-rate-with-45-49 true))
(defn estimated-sya-births [bundle _] (assoc bundle :estimated-sya-births true))
(defn scaling-factors [bundle _] (assoc bundle :scaling-factors true))
(defn actual-births [bundle _] (assoc bundle :actual-births true))
(defn at-risk-last-birth-year [bundle _] (assoc bundle :at-risk-last-birth-year true))
(defn estimated-births [bundle _] (assoc bundle :estimated-births true))
(defn at-risk-last-fert-last-year [bundle _] (assoc bundle :at-risk-last-fert-last-year true))
(defn at-risk-this-birth-year [bundle _] (assoc bundle :at-risk-this-birth-year true))

(comment "Smash the set of the function names through create-function to get default function defs")


(defn create-catalog
  [kw]
  {:witan/name kw
   :witan/fn (keyword (str "asfr-test/" (name kw)))
   :witan/version "1.0"})

(def catalog
  [{:witan/name :births, :witan/fn :asfr-test/births, :witan/version "1.0"}
   {:witan/name :get-births-data-year, :witan/fn :asfr-test/get-births-data-year, :witan/version "1.0"}
   {:witan/name :births-pool, :witan/fn :asfr-test/births-pool, :witan/version "1.0"}
   {:witan/name :estimated-sya-births-pool, :witan/fn :asfr-test/estimated-sya-births-pool, :witan/version "1.0"}
   {:witan/name :at-risk-this-fert-last-year, :witan/fn :asfr-test/at-risk-this-fert-last-year, :witan/version "1.0"}
   {:witan/name :fert-rate-without-45-49, :witan/fn :asfr-test/fert-rate-without-45-49, :witan/version "1.0"}
   {:witan/name :historic-fertility, :witan/fn :asfr-test/historic-fertility, :witan/version "1.0"}
   {:witan/name :fert-rate-with-45-49, :witan/fn :asfr-test/fert-rate-with-45-49, :witan/version "1.0"}
   {:witan/name :estimated-sya-births, :witan/fn :asfr-test/estimated-sya-births, :witan/version "1.0"}
   {:witan/name :in2, :witan/fn :asfr-test/in2, :witan/version "1.0" :witan/params {}}
   {:witan/name :scaling-factors, :witan/fn :asfr-test/scaling-factors, :witan/version "1.0"}
   {:witan/name :in3, :witan/fn :asfr-test/in3, :witan/version "1.0" :witan/params {}}
   {:witan/name :out, :witan/fn :asfr-test/out, :witan/version "1.0"}
   {:witan/name :actual-births, :witan/fn :asfr-test/actual-births, :witan/version "1.0"}
   {:witan/name :in4, :witan/fn :asfr-test/in4, :witan/version "1.0" :witan/params {}}
   {:witan/name :at-risk-last-birth-year, :witan/fn :asfr-test/at-risk-last-birth-year, :witan/version "1.0"}
   {:witan/name :estimated-births, :witan/fn :asfr-test/estimated-births, :witan/version "1.0"}
   {:witan/name :in1, :witan/fn :asfr-test/in1, :witan/version "1.0" :witan/params {}}
   {:witan/name :at-risk-last-fert-last-year, :witan/fn :asfr-test/at-risk-last-fert-last-year, :witan/version "1.0"}
   {:witan/name :at-risk-this-birth-year, :witan/fn :asfr-test/at-risk-this-birth-year, :witan/version "1.0"}])

(defn create-contracts
  [kw]
  {:witan/impl (keyword (str "witan.workspace-executor.acceptance.asfr-model/" (name kw)))
   :witan/fn (keyword (str "asfr-test/" (name kw)))
   :witan/version "1.0"})

(def contracts
  [{:witan/impl :witan.workspace-executor.acceptance.asfr-model/births, :witan/fn :asfr-test/births, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/get-births-data-year, :witan/fn :asfr-test/get-births-data-year, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/births-pool, :witan/fn :asfr-test/births-pool, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/estimated-sya-births-pool,
    :witan/fn :asfr-test/estimated-sya-births-pool,
    :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/at-risk-this-fert-last-year,
    :witan/fn :asfr-test/at-risk-this-fert-last-year,
    :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/fert-rate-without-45-49, :witan/fn :asfr-test/fert-rate-without-45-49, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/historic-fertility, :witan/fn :asfr-test/historic-fertility, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/fert-rate-with-45-49, :witan/fn :asfr-test/fert-rate-with-45-49, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/estimated-sya-births, :witan/fn :asfr-test/estimated-sya-births, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/pass-through, :witan/fn :asfr-test/in2, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/scaling-factors, :witan/fn :asfr-test/scaling-factors, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/pass-through, :witan/fn :asfr-test/in3, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/out, :witan/fn :asfr-test/out, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/actual-births, :witan/fn :asfr-test/actual-births, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/pass-through, :witan/fn :asfr-test/in4, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/at-risk-last-birth-year, :witan/fn :asfr-test/at-risk-last-birth-year, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/estimated-births, :witan/fn :asfr-test/estimated-births, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/pass-through, :witan/fn :asfr-test/in1, :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/at-risk-last-fert-last-year,
    :witan/fn :asfr-test/at-risk-last-fert-last-year,
    :witan/version "1.0"}
   {:witan/impl :witan.workspace-executor.acceptance.asfr-model/at-risk-this-birth-year, :witan/fn :asfr-test/at-risk-this-birth-year, :witan/version "1.0"}])


(def expected
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
  (let [workspace {:workflow witan-workflow
                   :catalog catalog
                   :contracts contracts}
        workspace' (s/with-fn-validation (wex/build! workspace))
        result (wex/run!! workspace' {})]
    (is (= expected (first result)))))
