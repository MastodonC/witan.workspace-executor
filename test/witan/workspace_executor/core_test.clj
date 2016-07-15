(ns witan.workspace-executor.core-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.workspace-executor.core :as wex]
            [witan.workspace-api :refer :all]))

(def FooNumber
  s/Num)

(def MulXParams
  {:x s/Num})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-data [x]
  ;; you could use this function to return some external data
  ;; but for testing we just return what was passed in
  x)

(defworkflowfn inc*
  {:witan/name :test.fn/inc
   :witan/version "1.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number]} _]
  {:number (inc number)})

(defworkflowfn mul2
  {:witan/name :test.fn/mul2
   :witan/version "1.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:* s/Num}
   :witan/param-schema {:as s/Keyword}}
  [{:keys [number]} {:keys [as] :or {as :number}}]
  (hash-map as (* number 2)))

(defworkflowfn mulX
  {:witan/name :test.fn/mulX
   :witan/version "1.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:* s/Num}
   :witan/param-schema  {:x s/Num
                         :as s/Keyword}}
  [{:keys [number]} {:keys [x as] :or {as :number}}]
  (hash-map as (* number x)))

(defworkflowfn add
  {:witan/name :test.fn/add
   :witan/version "1.0"
   :witan/input-schema  {:number s/Num
                         :to-add s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number to-add]} _]
  {:number (+ number to-add)})

(defworkflowfn ->str
  {:witan/name :test.fn/->str
   :witan/version "1.0"
   :witan/input-schema  {:thing s/Any}
   :witan/output-schema {:out-str s/Str}}
  [{:keys [thing]} _]
  {:out-str (str thing)})

(defworkflowfn rename
  {:witan/name :test.fn/rename
   :witan/version "1.0"
   :witan/input-schema  {:* s/Any}
   :witan/output-schema {:* s/Any}
   :witan/param-schema  {:from s/Keyword :to s/Keyword}}
  [msg {:keys [from to]}]
  (hash-map to (get msg from)))

(defworkflowpred finish?
  {:witan/name :test.pred/finish?
   :witan/version "1.0"
   :witan/input-schema  {:number s/Num}}
  [{:keys [number]} _]
  (> number 10))

(defworkflowfn param-spitter ;; TODO use defworkflowinput
  {:witan/name :test.in/param-spitter
   :witan/version "1.0"
   :witan/input-schema {:* s/Any}
   :witan/output-schema {:* s/Any}
   :witan/param-schema {:* s/Any}}
  [_ params]
  params)

(defworkflowfn msg-spitter
  {:witan/name :test.out/msg-spitter
   :witan/version "1.0"
   :witan/input-schema {:* s/Any}
   :witan/output-schema {:* s/Any}}
  [msg params]
  msg)

(defworkflowpred gte
  {:witan/name :test.out/gte
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/param-schema {:threshold s/Num}}
  [msg params]
  (>= (:number msg) (:threshold params)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def contracts
  [{:witan/fn      :foo/inc
    :witan/impl    :witan.workspace-executor.core-test/inc*
    :witan/version "1.0"}
   {:witan/fn      :foo/gte
    :witan/impl    :witan.workspace-executor.core-test/gte
    :witan/version "1.0"}
   {:witan/fn      :foo/mul2
    :witan/impl    :witan.workspace-executor.core-test/mul2
    :witan/version "1.0"}
   {:witan/fn      :foo/mulX
    :witan/impl    :witan.workspace-executor.core-test/mulX
    :witan/version "1.0"}
   {:witan/fn      :foo/add
    :witan/impl    :witan.workspace-executor.core-test/add
    :witan/version "1.0"}
   {:witan/fn      :foo/->str
    :witan/impl    :witan.workspace-executor.core-test/->str
    :witan/version "1.0"}
   {:witan/fn      :foo/rename
    :witan/impl    :witan.workspace-executor.core-test/rename
    :witan/version "1.0"}
   {:witan/fn      :foo/finish?
    :witan/impl    :witan.workspace-executor.core-test/finish?
    :witan/version "1.0"
    :witan/predicate? true
    :witan/inputs  [{:witan/schema       s/Any
                     :witan/key          :number
                     :witan/display-name "True if the number is > 10"}]}
   {:witan/fn :foo/putter
    :witan/impl :witan.workspace-executor.core-test/param-spitter
    :witan/version "1.0"}
   {:witan/fn :foo/printer
    :witan/impl :witan.workspace-executor.core-test/msg-spitter
    :witan/version "1.0"}])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest linear-one-step
  (testing "Very basic workspace"
    (let [workflow [[:in :a] [:a :out]]
          catalog [{:witan/name :in
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:number 1}}
                   {:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= [{:number 2}] result)))))

(deftest fan-out-one-step
  (testing "Basic workspace that has multiple outputs"
    (let [workflow [[:in :a] [:a :out] [:a :out2]]
          catalog [{:witan/name :in
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:number 1}}
                   {:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}
                   {:witan/name :out2
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= [{:number 2} {:number 2}] result)))))

(deftest merge-one-step
  (testing "Basic test that merges"
    (let [workflow [[:in :a] [:in2 :a] [:a :out]]
          catalog [{:witan/name :in
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:number 1}}
                   {:witan/name :in2
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:to-add 2}}
                   {:witan/name :a
                    :witan/fn :foo/add
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= [{:number 3 :to-add 2}] result)))))

(deftest diamond-test
  (testing "Workspace creates a diamond shape (fan then merge)"
    (let [workflow [[:in :a] [:a :b] [:a :c] [:b :d] [:c :d] [:d :out]]
          catalog [{:witan/name :in
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:number 1}}
                   {:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :b
                    :witan/fn :foo/mul2
                    :witan/version "1.0"
                    :witan/params {:as :blah}}
                   {:witan/name :c
                    :witan/fn :foo/mulX
                    :witan/version "1.0"
                    :witan/params {:x 3 :as :bliz}}
                   {:witan/name :d
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= result [{:number 3 :blah 4 :bliz 6}])))))

(deftest simple-loop
  (testing "Very basic workspace"
    (let [workflow [[:in :a] [:a [:gte :out :a]]]
          catalog [{:witan/name :in
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:number 1}}
                   {:witan/name :gte
                    :witan/fn :foo/gte
                    :witan/version "1.0"
                    :witan/params {:threshold 10}}
                   {:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= [{:number 10}] result)))))


(deftest gather-replay-nodes-test
  (testing "Tightest loop detection"
    (is
     (=
      []
      (wex/gather-replay-edges
       (wex/workflow->long-hand-workflow [[:in :a]
                                          [:a [:gte :out :a]]])
       {:name :gte :to [nil :a]}))))
  (testing "Tight loop detection"
    (is
     (=
      [[:in2 :b]]
      (wex/gather-replay-edges
       (wex/workflow->long-hand-workflow [[:in1 :a]
                                          [:in2 :b]
                                          [:a :b]
                                          [:b [:gte :out :a]]])
       {:name :gte :to [nil :a]}))))
  (testing "Longer loop leg"
    (is
     (=
      [[:in2 :b]]
      (wex/gather-replay-edges
       (wex/workflow->long-hand-workflow [[:in1 :a]
                                          [:in2 :b]
                                          [:a :b]
                                          [:b :c]
                                          [:c [:gte :out :a]]])
       {:name :gte :to [nil :a]}))))
  (testing "Inner loop"
    (is
     (=
      [[:in2 :b]]
      (wex/gather-replay-edges
       (wex/workflow->long-hand-workflow [[:in1 :a]
                                          [:in2 :b]
                                          [:a :b]
                                          [:b [:gte :c :a]]
                                          [:c [:gte :out :a]]])
       {:name :gte :to [nil :a]})))))

(deftest inner-outer-loop
  (let [wf (wex/workflow->long-hand-workflow
            [[:in1 :a]
             [:in2 :b]
             [:in4 :b]
             [:a :b]
             [:b :c]
             [:in3 :c]
             [:c [:gtex :d :b]]
             [:d [:gte :out :a]]])]
    (testing "Outer loop check should not include merges of inner"
      (is (= [[:in2 :b] [:in4 :b]]
             (wex/gather-replay-edges wf   {:name :gte :to [nil :a]}))))
    (testing "Inner loop check should not include merges of outer"
      (is (= [[:in3 :c]]
             (wex/gather-replay-edges wf   {:name :gtex :to [nil :b]}))))))

(deftest internal-merge-loop
  (testing "Loop with an internal merge"
    (is
     (=
      '([:in2 :b] [:in3 :c])
      (wex/gather-replay-edges
       (wex/workflow->long-hand-workflow [[:in1 :a]
                                          [:in2 :b]
                                          [:a :b]
                                          [:b :c]
                                          [:in3 :c]
                                          [:b :d] [:c :d]
                                          [:d [:gte :out :a]]])
       {:name :gte :to [nil :a]})))))

(deftest complex-1
  (testing "Merge within a loop"
    (let [workflow [[:in1 :a]
                    [:in2 :b]
                    [:a :b]
                    [:b [:gte :out :a]]]
          catalog [{:witan/name :in1
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:number 1}}
                   {:witan/name :in2
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:foo :bar}}
                   {:witan/name :gte
                    :witan/fn :foo/gte
                    :witan/version "1.0"
                    :witan/params {:threshold 10}}
                   {:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :b
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= [{:number 11 :foo :bar}] result)))))

(deftest param-replacement-needing-no-replay
  (let [workflow [[:in :a] [:a :out]]
        catalog [{:witan/name :in
                  :witan/fn :foo/putter
                  :witan/version "1.0"
                  :witan/params {:number 1}}
                 {:witan/name :a
                  :witan/fn :foo/inc
                  :witan/version "1.0"}
                 {:witan/name :out
                  :witan/fn :foo/printer
                  :witan/version "1.0"}]
        workspace {:workflow  workflow
                   :catalog   catalog
                   :contracts contracts}
        workspace-network (s/with-fn-validation (wex/build! workspace))
        result (wex/run!! workspace-network {})]
    (is result)
    (is (= [{:number 2}] result))
    (let [workspace' (update workspace
                             :catalog
                             #(cons
                               (assoc (first %)
                                      :witan/params
                                      {:number 10})
                               (rest %)))
          workspace-network' (wex/update-network! workspace-network workspace' [:in])
          result' (wex/run!! workspace-network' {})]
      (is result')
      (is (= [{:number 11}] result')))))

(deftest param-replacement-with-single-replay
  (let [workflow [[:in :m] [:m :out]]
        catalog [{:witan/name :m
                  :witan/fn :foo/mulX
                  :witan/version "1.0"
                  :witan/params {:x 2
                                 :as :number}}
                 {:witan/name :in
                  :witan/fn :foo/putter
                  :witan/version "1.0"
                  :witan/params {:number 1}}
                 {:witan/name :out
                  :witan/fn :foo/printer
                  :witan/version "1.0"}]
        workspace {:workflow  workflow
                   :catalog   catalog
                   :contracts contracts}
        workspace-network (s/with-fn-validation (wex/build! workspace))
        result (wex/run!! workspace-network {})]
    (is result)
    (is (= [{:number 2}] result))
    (let [workspace' (update workspace
                             :catalog
                             #(cons
                               (assoc (first %)
                                      :witan/params
                                      {:x 10
                                       :as :number})
                               (rest %)))
          workspace-network' (wex/update-network! workspace-network workspace' [:m])
          _ (wex/replay-nodes! workspace-network' [:m])
          result' (wex/await-results workspace-network')]
      (is result')
      (is (= [{:number 10}] result')))))

(deftest predicate-param-replacement-with-single-replay
  (let [workflow [[:in :a] [:a [:gte :out :a]]]
        catalog [{:witan/name :gte
                  :witan/fn :foo/gte
                  :witan/version "1.0"
                  :witan/params {:threshold 10}}
                 {:witan/name :a
                  :witan/fn :foo/inc
                  :witan/version "1.0"}
                 {:witan/name :in
                  :witan/fn :foo/putter
                  :witan/version "1.0"
                  :witan/params {:number 1}}
                 {:witan/name :out
                  :witan/fn :foo/printer
                  :witan/version "1.0"}]
        workspace {:workflow  workflow
                   :catalog   catalog
                   :contracts contracts}
        workspace-network (s/with-fn-validation (wex/build! workspace))
        result (wex/run!! workspace-network {})]
    (is result)
    (is (= [{:number 10}] result))
    (let [workspace' (update workspace
                             :catalog
                             #(cons
                               (assoc (first %)
                                      :witan/params
                                      {:threshold 20})
                               (rest %)))
          workspace-network' (wex/update-network! workspace-network workspace' [:gte])
          _ (wex/replay-nodes! workspace-network' [:gte])
          result' (wex/await-results workspace-network')]
      (is result')
      (is (= [{:number 20}] result')))))

(deftest exception-in-catalog-fn
  (testing "Basic workflow throws exception"
    (let [workflow [[:in :a] [:a :out]]
          catalog [{:witan/name :in
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:foo 1}}
                   {:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= 1 (count result)))
      (is (contains? (first result) :error)))))

(deftest exception-followed-by-a-fix
  (testing "Basic workflow throws exception"
    (let [workflow [[:in :a] [:a :out]]
          catalog [{:witan/name :in
                    :witan/fn :foo/putter
                    :witan/version "1.0"
                    :witan/params {:foo 1}}
                   {:witan/name :a
                    :witan/fn :foo/inc
                    :witan/version "1.0"}
                   {:witan/name :out
                    :witan/fn :foo/printer
                    :witan/version "1.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace-network (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace-network {})]
      (is result)
      (is (= 1 (count result)))
      (is (contains? (first result) :error))
      (let [workspace' (update workspace
                               :catalog
                               #(cons
                                 (assoc (first %)
                                        :witan/params
                                        {:number 1})
                                 (rest %)))
            workspace-network' (wex/update-network! workspace-network workspace' [:in])
            result' (wex/run!! workspace-network' {})]
        (is result')
        (is (= [{:number 2}] result'))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#_(deftest valid-workspace-tests
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
