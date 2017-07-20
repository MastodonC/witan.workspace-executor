(ns witan.workspace-executor.core-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.workspace-executor.core :as wex]
            [witan.workspace-api :refer :all]
            [witan.workspace-api.utils :refer [map-fn-meta]]))

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
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number]} _]
  {:number (inc number)})

(defworkflowfn inc2*
  {:witan/name :test.fn/inc2
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:number2 s/Num}}
  [{:keys [number]} _]
  {:number2 (inc number)})

(defworkflowfn inc3*
  {:witan/name :test.fn/inc3
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:number s/Str}}
  [{:keys [number]} _]
  {:number (str (inc number))})

(defworkflowfn mul2
  {:witan/name :test.fn/mul2
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number]} _]
  {:number (* number 2)})

(defworkflowfn mulX
  {:witan/name :test.fn/mulX
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:number s/Num}
   :witan/param-schema  {:x s/Num}}
  [{:keys [number]} {:keys [x]}]
  {:number (* number x)})

(defworkflowfn mulX-to-add
  {:witan/name :test.fn/mulX-to-add
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:to-add s/Num}
   :witan/param-schema  {:x s/Num}}
  [{:keys [number]} {:keys [x]}]
  {:to-add (* number x)})

(defworkflowfn add
  {:witan/name :test.fn/add
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num
                         :to-add s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number to-add]} _]
  {:number (+ number to-add)})

(defworkflowfn ->str
  {:witan/name :test.fn/->str
   :witan/version "1.0.0"
   :witan/input-schema  {:thing s/Any}
   :witan/output-schema {:out-str s/Str}}
  [{:keys [thing]} _]
  {:out-str (str thing)})

(defworkflowfn rename
  {:witan/name :test.fn/rename
   :witan/version "1.0.0"
   :witan/input-schema  {:* s/Any}
   :witan/output-schema {:* s/Any}
   :witan/param-schema  {:from s/Keyword :to s/Keyword}}
  [msg {:keys [from to]}]
  (hash-map to (get msg from)))

(defworkflowpred finish?
  {:witan/name :test.pred/finish?
   :witan/version "1.0.0"
   :witan/input-schema  {:number s/Num}}
  [{:keys [number]} _]
  (> number 10))

(definput param-spitter-number
  {:witan/name :test.in/param-spitter-number
   :witan/version "1.0.0"
   :witan/key :number
   :witan/schema s/Num})

(definput param-spitter-to-add
  {:witan/name :test.in/param-spitter-to-add
   :witan/version "1.0.0"
   :witan/key :to-add
   :witan/schema s/Num})

(definput param-spitter-foo
  {:witan/name :test.in/param-spitter-foo
   :witan/version "1.0.0"
   :witan/key :foo
   :witan/schema s/Str})

(defworkflowoutput out-spitter-number
  {:witan/name :test.out/out-spitter-number
   :witan/version "1.0.0"
   :witan/input-schema {:number s/Num}}
  [d _] d)

(defworkflowpred gte
  {:witan/name :test.pred/gte
   :witan/version "1.0.0"
   :witan/input-schema {:number s/Num}
   :witan/param-schema {:threshold s/Num}}
  [msg params]
  (>= (:number msg) (:threshold params)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def contracts
  (map-fn-meta
   inc*
   inc2*
   inc3*
   mul2
   mulX
   mulX-to-add
   add
   rename
   ->str
   finish?
   param-spitter-number
   param-spitter-to-add
   param-spitter-foo
   out-spitter-number
   gte))

(deftest workflow->long-hand-workflow
  (testing "Simple workflow"
    (is
     (= [(wex/map->Node {:name :in,
                         :outbound nil,
                         :inbound nil,
                         :from nil,
                         :to [:a],
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :source}),
         (wex/map->Node {:name :a,
                         :outbound nil,
                         :inbound nil,
                         :from [:in],
                         :to [:out],
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :from-one}),
         (wex/map->Node {:name :out,
                         :outbound nil,
                         :inbound nil,
                         :from [:a],
                         :to nil,
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :sink})]
        (wex/workflow->long-hand-workflow [[:in :a] [:a :out]]))))
  (testing "Merge workflow"
    (is
     (= [(wex/map->Node {:name :in,
                         :outbound nil,
                         :inbound nil,
                         :from nil,
                         :to [:a],
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :source})
         (wex/map->Node {:name :in2,
                         :outbound nil,
                         :inbound nil,
                         :from nil,
                         :to [:a],
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :source}),
         (wex/map->Node {:name :a,
                         :outbound nil,
                         :inbound nil,
                         :from [:in :in2],
                         :to [:out],
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :from-many}),
         (wex/map->Node {:name :out,
                         :outbound nil,
                         :inbound nil,
                         :from [:a],
                         :to nil,
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :sink})]
        (wex/workflow->long-hand-workflow [[:in :a] [:in2 :a] [:a :out]]))))
  (testing "Predicate workflow"
    (is
     (= [(wex/map->Node {:name :in,
                         :outbound nil,
                         :inbound nil,
                         :from nil,
                         :to [:a],
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :source}),
         (wex/map->Node {:name :a,
                         :outbound nil,
                         :inbound nil,
                         :from [:in],
                         :to [:gte],
                         :target-of :gte
                         :pred? nil,
                         :error-ch nil
                         :type :from-one})
         (wex/map->Node {:name :gte,
                         :outbound nil,
                         :inbound nil,
                         :from [:a],
                         :to [:out :a],
                         :target-of nil,
                         :pred? true
                         :error-ch nil
                         :type :predicate}),
         (wex/map->Node {:name :out,
                         :outbound nil,
                         :inbound nil,
                         :from nil
                         :to nil,
                         :target-of :gte
                         :pred? nil,
                         :error-ch nil
                         :type :sink})]
        (wex/workflow->long-hand-workflow [[:in :a] [:a [:gte :out :a]]]))))
  (testing "Predicate workflow with long tail"
    (is
     (= [(wex/map->Node {:name :in,
                         :outbound nil,
                         :inbound nil,
                         :from nil,
                         :to [:a],
                         :target-of nil,
                         :pred? nil,
                         :error-ch nil
                         :type :source}),
         (wex/map->Node {:name :a,
                         :outbound nil,
                         :inbound nil,
                         :from [:in],
                         :to [:gte],
                         :target-of :gte
                         :pred? nil,
                         :error-ch nil
                         :type :from-one})
         (wex/map->Node {:name :gte,
                         :outbound nil,
                         :inbound nil,
                         :from [:a],
                         :to [:out :a],
                         :target-of nil,
                         :pred? true
                         :error-ch nil
                         :type :predicate}),
         (wex/map->Node {:name :b,
                         :outbound nil,
                         :inbound nil,
                         :from nil
                         :to [:out],
                         :target-of :gte
                         :pred? nil,
                         :error-ch nil
                         :type :from-one})
         (wex/map->Node {:name :out,
                         :outbound nil,
                         :inbound nil,
                         :from [:b]
                         :to nil,
                         :target-of nil
                         :pred? nil,
                         :error-ch nil
                         :type :sink})]
        (wex/workflow->long-hand-workflow [[:in :a] [:a [:gte :b :a]] [:b :out]]))
     "This demonstrates a bug where `:b` should *not* be listed as a `:type :source` because it actually has an upstream task `:a`")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest linear-one-step
  (testing "Very basic workspace"
    (let [workflow [[:in :a] [:a :out]]
          catalog [{:witan/name :in
                    :witan/fn :test.in/param-spitter-number
                    :witan/version "1.0.0"
                    :witan/type :input
                    :witan/params {:fn (constantly 1)
                                   :src "foo"}}
                   {:witan/name :a
                    :witan/fn :test.fn/inc
                    :witan/version "1.0.0"
                    :witan/type :function}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/version "1.0.0"
                    :witan/type :output}]
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
                    :witan/fn :test.in/param-spitter-number
                    :witan/version "1.0.0"
                    :witan/params {:fn (constantly 1)
                                   :src "foo"}
                    :witan/type :input}
                   {:witan/name :a
                    :witan/fn :test.fn/inc
                    :witan/version "1.0.0"
                    :witan/type :function}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/version "1.0.0"
                    :witan/type :output}
                   {:witan/name :out2
                    :witan/fn :test.out/out-spitter-number
                    :witan/version "1.0.0"
                    :witan/type :output}]
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
                    :witan/fn :test.in/param-spitter-number
                    :witan/version "1.0.0"
                    :witan/params {:fn (constantly 1)
                                   :src "foo"}
                    :witan/type :input}
                   {:witan/name :in2
                    :witan/fn :test.in/param-spitter-to-add
                    :witan/version "1.0.0"
                    :witan/params {:fn (constantly 2)
                                   :src "foo"}
                    :witan/type :function}
                   {:witan/name :a
                    :witan/fn :test.fn/add
                    :witan/version "1.0.0"
                    :witan/type :function}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/version "1.0.0"
                    :witan/type :output}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= [{:number 3}] result)))))

(deftest diamond-test
  (testing "Workspace creates a diamond shape (fan then merge)"
    (let [workflow [[:in :a] [:a :b] [:a :c] [:b :d] [:c :d] [:d :out]]
          catalog [{:witan/name :in
                    :witan/fn :test.in/param-spitter-number
                    :witan/type :input
                    :witan/version "1.0.0"
                    :witan/params {:fn (constantly 1)
                                   :src "foo"}}
                   {:witan/name :a
                    :witan/fn :test.fn/inc
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :b
                    :witan/fn :test.fn/mul2
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :c
                    :witan/fn :test.fn/mulX-to-add
                    :witan/type :function
                    :witan/version "1.0.0"
                    :witan/params {:x 3}}
                   {:witan/name :d
                    :witan/fn :test.fn/add
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/type :output
                    :witan/version "1.0.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= result [{:number 10}])))))

(deftest simple-loop
  (testing "Very basic workspace"
    (let [workflow [[:in :a] [:a [:gte :out :a]]]
          catalog [{:witan/name :in
                    :witan/fn :test.in/param-spitter-number
                    :witan/type :input
                    :witan/version "1.0.0"
                    :witan/params {:fn (constantly 1)
                                   :src "foo"}}
                   {:witan/name :gte
                    :witan/fn :test.pred/gte
                    :witan/version "1.0.0"
                    :witan/type :predicate
                    :witan/params {:threshold 10}}
                   {:witan/name :a
                    :witan/fn :test.fn/inc
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/type :output
                    :witan/version "1.0.0"}]
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
      #{}
      (wex/gather-replay-edges
       (wex/workflow->long-hand-workflow [[:in :a]
                                          [:a [:gte :out :a]]])
       {:name :gte :to [nil :a]}))))
  (testing "Tight loop detection"
    (is
     (=
      #{[:in2 :b]}
      (wex/gather-replay-edges
       (wex/reduce-nodes-to-map-name->node
        (wex/workflow->long-hand-workflow [[:in1 :a]
                                           [:in2 :b]
                                           [:a :b]
                                           [:b [:gte :out :a]]]))
       {:name :gte :to [nil :a]}))))
  (testing "Longer loop leg"
    (is
     (=
      #{[:in2 :b]}
      (wex/gather-replay-edges
       (wex/reduce-nodes-to-map-name->node
        (wex/workflow->long-hand-workflow [[:in1 :a]
                                           [:in2 :b]
                                           [:a :b]
                                           [:b :c]
                                           [:c [:gte :out :a]]]))
       {:name :gte :to [nil :a]}))))
  (testing "Inner loop"
    (is
     (=
      #{[:in2 :b]}
      (wex/gather-replay-edges
       (wex/reduce-nodes-to-map-name->node
        (wex/workflow->long-hand-workflow [[:in1 :a]
                                           [:in2 :b]
                                           [:a :b]
                                           [:b [:gte :c :a]]
                                           [:c [:gte :out :a]]]))
       {:name :gte :to [nil :a]})))))

(deftest inner-outer-loop
  (let [wf (wex/reduce-nodes-to-map-name->node
            (wex/workflow->long-hand-workflow
             [[:in1 :a]
              [:in2 :b]
              [:in4 :b]
              [:a :b]
              [:b :c]
              [:in3 :c]
              [:c [:gtex :d :b]]
              [:d [:gte :out :a]]]))]
    (testing "Outer loop check should not include merges of inner"
      (is (= #{[:in2 :b] [:in4 :b]}
             (wex/gather-replay-edges wf   {:name :gte :to [nil :a]}))))
    (testing "Inner loop check should not include merges of outer"
      (is (= #{[:in3 :c]}
             (wex/gather-replay-edges wf   {:name :gtex :to [nil :b]}))))))

(deftest internal-merge-loop
  (testing "Loop with an internal merge"
    (is
     (=
      #{[:in2 :b] [:in3 :c]}
      (wex/gather-replay-edges
       (wex/reduce-nodes-to-map-name->node
        (wex/workflow->long-hand-workflow [[:in1 :a]
                                           [:in2 :b]
                                           [:a :b]
                                           [:b :c]
                                           [:in3 :c]
                                           [:b :d] [:c :d]
                                           [:d [:gte :out :a]]]))
       {:name :gte :to [nil :a]})))))

(deftest complex-1
  (testing "Merge within a loop"
    (let [workflow [[:in1 :a]
                    [:in2 :b]
                    [:b :c]
                    [:a :c]
                    [:c [:gte :out :a]]]
          catalog [{:witan/name :in1
                    :witan/fn :test.in/param-spitter-number
                    :witan/type :input
                    :witan/version "1.0.0"
                    :witan/params {:fn (constantly 1)
                                   :src "foo"}}
                   {:witan/name :in2
                    :witan/fn :test.in/param-spitter-number
                    :witan/type :input
                    :witan/version "1.0.0"
                    :witan/params {:fn (constantly 4)
                                   :src "foo"}}
                   {:witan/name :gte
                    :witan/fn :test.pred/gte
                    :witan/type :predicate
                    :witan/version "1.0.0"
                    :witan/params {:threshold 15}}
                   {:witan/name :a
                    :witan/fn :test.fn/inc
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :b
                    :witan/fn :test.fn/mulX-to-add
                    :witan/type :function
                    :witan/version "1.0.0"
                    :witan/params {:x 3}}
                   {:witan/name :c
                    :witan/fn :test.fn/add
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/type :output
                    :witan/version "1.0.0"}]
          workspace {:workflow  workflow
                     :catalog   catalog
                     :contracts contracts}
          workspace' (s/with-fn-validation (wex/build! workspace))
          result (wex/run!! workspace' {})]
      (is result)
      (is (= [{:number 27}] result)))))

(deftest param-replacement-needing-no-replay
  (let [workflow [[:in :a] [:a :out]]
        catalog [{:witan/name :in
                  :witan/fn :test.in/param-spitter-number
                  :witan/type :input
                  :witan/version "1.0.0"
                  :witan/params {:fn (constantly 1)
                                 :src "foo"}}
                 {:witan/name :a
                  :witan/fn :test.fn/inc
                  :witan/type :function
                  :witan/version "1.0.0"}
                 {:witan/name :out
                  :witan/fn :test.out/out-spitter-number
                  :witan/type :output
                  :witan/version "1.0.0"}]
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
                                      {:fn (constantly 10)
                                       :src "foo"})
                               (rest %)))
          workspace-network' (wex/update-network! workspace-network workspace' [:in])
          result' (wex/run!! workspace-network' {})]
      (is result')
      (is (= [{:number 11}] result')))))

(deftest param-replacement-with-single-replay
  (let [workflow [[:in :m] [:m :out]]
        catalog [{:witan/name :m
                  :witan/type :function
                  :witan/fn :test.fn/mulX
                  :witan/version "1.0.0"
                  :witan/params {:x 2}}
                 {:witan/name :in
                  :witan/fn :test.in/param-spitter-number
                  :witan/type :input
                  :witan/version "1.0.0"
                  :witan/params {:fn (constantly 1)
                                 :src "foo"}}
                 {:witan/name :out
                  :witan/fn :test.out/out-spitter-number
                  :witan/type :output
                  :witan/version "1.0.0"}]
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
                                      {:x 10})
                               (rest %)))
          workspace-network' (wex/update-network! workspace-network workspace' [:m])
          _ (wex/replay-nodes! workspace-network' [:m])
          result' (wex/await-results workspace-network')]
      (is result')
      (is (= [{:number 10}] result')))))

(deftest predicate-param-replacement-with-single-replay
  (let [workflow [[:in :a] [:a [:gte :out :a]]]
        catalog [{:witan/name :gte
                  :witan/fn :test.pred/gte
                  :witan/type :predicate
                  :witan/version "1.0.0"
                  :witan/params {:threshold 10}}
                 {:witan/name :a
                  :witan/fn :test.fn/inc
                  :witan/type :function
                  :witan/version "1.0.0"}
                 {:witan/name :in
                  :witan/fn :test.in/param-spitter-number
                  :witan/type :input
                  :witan/version "1.0.0"
                  :witan/params {:fn (constantly 1)
                                 :src "foo"}}
                 {:witan/name :out
                  :witan/fn :test.out/out-spitter-number
                  :witan/type :output
                  :witan/version "1.0.0"}]
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
                    :witan/fn :test.in/param-spitter-number
                    :witan/type :input
                    :witan/version "1.0.0"
                    :witan/params {:foo 1}}
                   {:witan/name :a
                    :witan/fn :test.fn/inc
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/type :output
                    :witan/version "1.0.0"}]
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
                    :witan/fn :test.in/param-spitter-number
                    :witan/type :input
                    :witan/version "1.0.0"
                    :witan/params {:fn inc
                                   :src "foo"}}
                   {:witan/name :a
                    :witan/fn :test.fn/inc
                    :witan/type :function
                    :witan/version "1.0.0"}
                   {:witan/name :out
                    :witan/fn :test.out/out-spitter-number
                    :witan/type :output
                    :witan/version "1.0.0"}]
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
                                        {:fn (constantly 1)
                                         :src "foo"})
                                 (rest %)))
            workspace-network' (wex/update-network! workspace-network workspace' [:in])
            result' (wex/run!! workspace-network' {})]
        (is result')
        (is (= [{:number 2}] result'))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest valid-workspace-tests
  (testing "Workspace is valid"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :b
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}]
                     :contracts contracts}]
      (is (nil? (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - unrepresented workflow node"
    (let [workspace {:workflow [[:a :b] [:b :c]] ;; <<< There is no :c in the catalog
                     :catalog [{:witan/name :a
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :b
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^One or more workflow entries are unrepresented in the catalog: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no catalog combination"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "2.0.0" ;; <<< There is no contract for version 2.0
                                }
                               {:witan/name :b
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^There are no contracts for the following function \+ version combinations: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no input fulfilment @ key level - no upstream, input will not be fulfilled"
    (let [workspace {:workflow [[:a :c] [:b :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :b
                                :witan/fn :test.fn/inc2
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :c
                                :witan/fn :test.fn/add
                                :witan/type :function
                                :witan/version "1.0.0"}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"The following nodes will not receive the required inputs from their parent nodes: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid - no input fulfilment @ schema level - no upstream, input will not be fulfilled"
    (let [workspace {:workflow [[:a :c] [:b :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :b
                                :witan/fn :test.fn/inc3
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :c
                                :witan/fn :test.fn/add
                                :witan/type :function
                                :witan/version "1.0.0"}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"The following nodes will not receive the required inputs from their parent nodes: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))
  (testing "Invalid - no input fulfilment - inputs will clash"
    (let [workspace {:workflow [[:a :c] [:b :c]]
                     :catalog [{:witan/name :a
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :b
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :c
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}] ;; <<< Both :a and :b provide 'number' - suggest output mapping.
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^The following nodes will experience a clash of inputs from their parents: .*"
           (s/with-fn-validation (wex/validate-workspace workspace))))))

  (testing "Invalid  - duplicates in the catalog"
    (let [workspace {:workflow [[:a :b]]
                     :catalog [{:witan/name :a
                                :witan/fn :test.fn/inc
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :b
                                :witan/fn :test.fn/mul2
                                :witan/type :function
                                :witan/version "1.0.0"}
                               {:witan/name :b ;; <<< :b is duplicated
                                :witan/fn :test.fn/mul2
                                :witan/type :function
                                :witan/version "1.0.0"}]
                     :contracts contracts}]
      (is (thrown-with-msg?
           IllegalArgumentException
           #"^One or more catalog entries are duplicated: .*"
           (s/with-fn-validation (wex/validate-workspace workspace)))))))


(deftest viewing-test
  (testing "does a workflow produce valid graphviz?"
    (let [result (wex/workflow->graphviz [[:a :b] [:b :c]])]
      (is (= {:a '(:b), :b '(:c), :c '()} result)))))
