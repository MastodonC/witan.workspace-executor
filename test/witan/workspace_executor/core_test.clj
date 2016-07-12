(ns witan.workspace-executor.core-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.workspace-executor.core :as wex]))

(def FooNumber
  s/Num)

(def MulXParams
  {:x s/Num})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-data [x]
  ;; you could use this function to return some external data
  ;; but for testing we just return what was passed in
  x)

(defn inc*
  [{:keys [number]} _]
  (println "inc" number)
  {:number (inc number)})

(defn mul2
  [{:keys [number]} _]
  (println "mul2" number)
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

(defn finish?
  [{:keys [number]} _]
  (println "Finish?" (> number 10))
  (> number 10))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def contracts
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
                     :witan/display-name "String representation"}]}
   {:witan/fn      :foo/finish?
    :witan/impl    'witan.workspace-executor.core-test/finish?
    :witan/version "1.0"
    :witan/predicate? true
    :witan/inputs  [{:witan/schema       s/Any
                     :witan/key          :number
                     :witan/display-name "True if the number is > 10"}]}])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest happy-path-tests-1
  (testing "Happy path (linear) test"
    (let [workflow [[:a :b] [:b :c]]
          catalog [{:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                    :witan/input-src-key  1
                                    :witan/input-dest-key :number}]}
                   {:witan/name :b
                    :witan/fn :foo/mul2
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-key :number}]}
                   {:witan/name :c
                    :witan/fn :foo/mulX
                    :witan/version "1.0"
                    :witan/params {:x 3}
                    :witan/inputs [{:witan/input-src-key :number}]}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          result (s/with-fn-validation (wex/execute workspace))]
      (is result)
      (is (= result {:number 12})))))

(deftest happy-path-tests-2
  (testing "Happy path (merge w/output mapping) test"
    (let [workflow [[:a :c] [:b :c] [:a :d]]
          catalog [{:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                    :witan/input-src-key  1
                                    :witan/input-dest-key :number}]}
                   {:witan/name :b
                    :witan/fn :foo/mul2
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                    :witan/input-src-key  2
                                    :witan/input-dest-key :number}]
                    :witan/outputs [{:witan/output-src-key :number
                                     :witan/output-dest-key :to-add}]}
                   {:witan/name :c
                    :witan/fn :foo/add
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-key :number}
                                   {:witan/input-src-key :to-add}]}
                   {:witan/name :d
                    :witan/fn :foo/inc
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-key :number}]
                    :witan/outputs [{:witan/output-src-key :number
                                     :witan/output-dest-key :number2}]}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          result (s/with-fn-validation (wex/execute workspace))]
      (is result)
      (is (= result {:number 6 :number2 3})))))

(deftest happy-path-tests-3
  (testing "Happy path test with a loop"
    (let [workflow [[:a [:finish? :b :a]]]
          catalog [{:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                    :witan/input-src-key  1
                                    :witan/input-dest-key :number}]}
                   {:witan/name :b
                    :witan/fn :foo/mul2
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                    :witan/input-src-key  2
                                    :witan/input-dest-key :number}]}
                   {:witan/name :finish?
                    :witan/fn :foo/finish?
                    :witan/version "1.0"
                    :witan/inputs [{:witan/input-src-key :number}]}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          result (s/with-fn-validation (wex/execute workspace))]
      (is result)
      (is (= result {:number 6 :number2 3})))))

(deftest valid-workspace-tests
  (testing "Workspace is valid"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  :1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}]}]
                     :contracts contracts}]
      (is (nil? (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - duplicate inputs in the contract"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  :1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}]}]
                     :contracts [{:witan/fn      :foo/inc
                                  :witan/impl    'witan.workspace-executor.core-test/inc*
                                  :witan/version "1.0"
                                  :witan/params-schema nil
                                  :witan/inputs  [{:witan/schema       FooNumber
                                                   :witan/key          :number
                                                   :witan/display-name "Number1"}
                                                  {:witan/schema       FooNumber
                                                   :witan/key          :number ;; <<< This is a duplication
                                                   :witan/display-name "Number2"}]
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
                                                   :witan/display-name "Number"}]}]}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^One or more contracts have duplicated inputs: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - duplicate outputs in the contract"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  :1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}]}]
                     :contracts [{:witan/fn      :foo/inc
                                  :witan/impl    'witan.workspace-executor.core-test/inc*
                                  :witan/version "1.0"
                                  :witan/params-schema nil
                                  :witan/inputs  [{:witan/schema       FooNumber
                                                   :witan/key          :number
                                                   :witan/display-name "Number1"}]
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
                                                   :witan/display-name "Number"}
                                                  {:witan/schema       FooNumber
                                                   :witan/key          :number ;; <<< This is a duplication
                                                   :witan/display-name "Number2"}]}]}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^One or more contracts have duplicated outputs: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - unrepresented workflow node"
    (let [workspace {:workflow [[:a :b] [:b :c]] ;; <<< There is no :c in the catalog
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  :1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}]}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^One or more workflow entries are unrepresented in the catalog: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no catalog combination"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "2.0" ;; <<< There is no contract for version 2.0
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  :1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}]}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^There are no contracts for the following function \+ version combinations: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no input fulfilment - input is missing"
    (let [workspace {:workflow [[:a :c] [:b :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :foobar}] ;; <<< No reference to ':number' input in contract
                                :witan/outputs [{:witan/output-src-key :number
                                                 :witan/output-dest-key :to-add}]}
                               {:witan/name :c
                                :witan/fn :foo/add
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}
                                               {:witan/input-src-key :to-add}]}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^The following nodes are either missing a reference to an input or provide an unexpected input: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no input fulfilment - has extra input"
    (let [workspace {:workflow [[:a :c] [:b :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :number}
                                               {:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :foo}]
                                :witan/outputs [{:witan/output-src-key :number
                                                 :witan/output-dest-key :to-add}]}
                               {:witan/name :c
                                :witan/fn :foo/add
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}
                                               {:witan/input-src-key :to-add}]}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^The following nodes are either missing a reference to an input or provide an unexpected input: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no input fulfilment - no upstream, input will not be fulfilled"
    (let [workspace {:workflow [[:a :c] [:b :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}] ;; <<< :b has no parent nodes that will fulfil this
                                :witan/outputs [{:witan/output-src-key :number
                                                 :witan/output-dest-key :to-add}]}
                               {:witan/name :c
                                :witan/fn :foo/add
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}
                                               {:witan/input-src-key :to-add}]}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"The following nodes will not receive the required inputs from their parent nodes: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no input fulfilment - has upstream but doesn't fulfil"
    (let [workspace {:workflow [[:a :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :c
                                :witan/fn :foo/add
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}
                                               {:witan/input-src-key :to-add}]}] ;; <<< Nothing fulfils :to-add
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"The following nodes will not receive the required inputs from their parent nodes: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no input fulfilment - inputs will clash"
    (let [workspace {:workflow [[:a :c] [:b :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  2
                                                :witan/input-dest-key :number}]}
                               {:witan/name :c
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key :number}]}] ;; <<< Both :a and :b provide 'number' - suggest output mapping.
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^The following nodes will experience a clash of inputs from their parents \(consider using output mappings\): .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid  - duplicates in the catalog"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :foo/inc
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                                                :witan/input-src-key  1
                                                :witan/input-dest-key :number}]}
                               {:witan/name :b
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key  :number}]}
                               {:witan/name :b ;; <<< :b is duplicated
                                :witan/fn :foo/mul2
                                :witan/version "1.0"
                                :witan/inputs [{:witan/input-src-key  :number}]}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^One or more catalog entries are duplicated: .*"
           (s/with-fn-validation (wex/validate-workspace workspace)))))))
