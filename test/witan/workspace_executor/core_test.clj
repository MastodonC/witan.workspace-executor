(ns witan.workspace-executor.core-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.workspace-executor.core :refer [execute]]))

(def FooNumber
  s/Num)

(def MulXParams
  {:x s/Num})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-data [x] (get {:a 1 :b 2} x 3))

(defn inc*
  [{:keys [number]} _]
  {:number (inc number)})

(defn mul2
  [{:keys [number]} _]
  {:number (* number 2)})

(defn mulX
  [{:keys [number]} {:keys [x]}]
  {:number (* number x)})

(defn add
  [{:keys [number to-add]} _]
  {:number (+ number to-add)})

(defn ->str
  [{:keys [thing]} _]
  {:out-str (str thing)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def happy-contracts
  [{:witan/fn      :foo/inc
    :witan/impl    'witan.workspace-executor.core-test/inc*
    :witan/version "1.0"
    :witan/params-schema nil
    :witan/inputs  [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]
    :witan/outputs [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]}
   {:witan/fn      :foo/mul2
    :witan/impl    'witan.workspace-executor.core-test/mul2
    :witan/version "1.0"
    :witan/params-schema nil
    :witan/inputs  [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]
    :witan/outputs [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]}
   {:witan/fn      :foo/mulX
    :witan/impl    'witan.workspace-executor.core-test/mulX
    :witan/version "1.0"
    :witan/params-schema MulXParams
    :witan/inputs  [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]
    :witan/outputs [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]}
   {:witan/fn      :foo/add
    :witan/impl    'witan.workspace-executor.core-test/add
    :witan/version "1.0"
    :witan/params-schema nil
    :witan/inputs  [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}
                    {:witan/schema       FooNumber
                     :witan/key          :to-add
                     :witan/display-name "To add"}]
    :witan/outputs [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]}

   {:witan/fn      :foo/->str
    :witan/impl    'witan.workspace-executor.core-test/->str
    :witan/version "1.0"
    :witan/inputs  [{:witan/schema       s/Any
                     :witan/key          :thing
                     :witan/display-name "The value we want to string-ify"}]
    :witan/outputs [{:witan/schema       s/Str
                     :witan/key          :out-str
                     :witan/display-name "String representation"}]}])

(def happy-catalog
  [{:witan/name :a
    :witan/fn :foo/inc
    :witan/version "1.0"
    :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                    :witan/input-src-key  :a
                    :witan/input-dest-key :number}]}
   {:witan/name :b
    :witan/fn :foo/mul2
    :witan/version "1.0"
    :witan/inputs [{:witan/input-src-key :number}]}
   {:witan/name :c
    :witan/fn :foo/mulX
    :witan/version "1.0"
    :witan/params {:x 3}
    :witan/inputs [{:witan/input-src-key :number}]}
   {:witan/name :d
    :witan/fn :foo/add
    :witan/version "1.0"
    :witan/inputs [{:witan/input-src-key :number}
                   {:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                    :witan/input-src-key  :b
                    :witan/input-dest-key :to-add}]}
   {:witan/name :e
    :witan/fn :foo/->str
    :witan/version "1.0"
    :witan/inputs [{:witan/input-src-key :number
                    :witan/input-dest-key :thing}]}])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest happy-path-linear-test
  (testing "Happy path (linear) test"
    (let [workflow [:a :b :b :c :c :d :d :e]
          workspace {:workflow  workflow
                     :contracts happy-contracts
                     :catalog   happy-catalog}
          result (s/with-fn-validation (execute workspace))]
      (is result)
      (is (= (-> result keys first) (last workflow)))
      (is (contains? (-> result vals first) :out-str))
      (is (= (-> result vals first :out-str) "14")))))
