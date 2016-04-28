(ns witan.workspace-executor.function-catalog
  (:require [schema.core :as s]))

(defn gte-ten
  [_ _ {:keys [number] :as msg} _]
  (<= 10 number))

(defn my-inc
  [{:keys [number] :as msg}]
  (update msg
          :number
          inc))

(defn mult
  [{:keys [number] :as msg}]
  (->
   msg
   (assoc :mult 2)
   (dissoc :number)))

(defn sum
  [{:keys [number mult] :as msg}]
  (->
   msg
   (assoc :number (* number mult))
   (dissoc :mult)))

(defn mul2
  [{:keys [number]}]
  {:number (* number 2)})

(defn mulX
  [{:keys [number]} {:keys [x]}]
  {:number (* number x)})

(defn add
  [{:keys [number to-add]}]
  {:number (+ number to-add)})

(defn ->str
  [{:keys [thing]}]
  {:out-str (str thing)})

(defn source-data
  []
  [{:number 0}])

(defn enough?
  [{:keys [number]}]
  (> number 10))


#_(def contracts
  [{:witan/fn      :foo/inc
    :witan/impl    'witan.workspace-executor.function-catalog/inc*
    :witan/version "1.0"
    :witan/params-schema nil
    :witan/inputs  [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]
    :witan/outputs [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]}
   {:witan/fn      :foo/mul2
    :witan/impl    'witan.workspace-executor.function-catalog/mul2
    :witan/version "1.0"
    :witan/params-schema nil
    :witan/inputs  [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]
    :witan/outputs [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]}
   {:witan/fn      :foo/mulX
    :witan/impl    'witan.workspace-executor.function-catalog/mulX
    :witan/version "1.0"
    :witan/params-schema MulXParams
    :witan/inputs  [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]
    :witan/outputs [{:witan/schema       FooNumber
                     :witan/key          :number
                     :witan/display-name "Number"}]}
   {:witan/fn      :foo/add
    :witan/impl    'witan.workspace-executor.function-catalog/add
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
    :witan/impl    'witan.workspace-executor.function-catalog/->str
    :witan/version "1.0"
    :witan/inputs  [{:witan/schema       s/Any
                     :witan/key          :thing
                     :witan/display-name "The value we want to string-ify"}]
    :witan/outputs [{:witan/schema       s/Str
                     :witan/key          :out-str
                     :witan/display-name "String representation"}]}])

(def catalog [{:witan/name :inc
               :witan/fn :witan.workspace-executor.function-catalog/my-inc
               :witan/version "1.0"
               :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                               :witan/input-src-key  1
                               :witan/input-dest-key :number}]}
              {:witan/name :source-data
               :witan/fn :witan.workspace-executor.function-catalog/source-data
               :witan/version "1.0"
               :witan/inputs [{:witan/input-src-fn   'witan.workspace-executor.core-test/get-data
                               :witan/input-src-key  1
                               :witan/input-dest-key :number}]}              
              {:witan/name :mul2
               :witan/fn :witan.workspace-executor.function-catalog/mul2
               :witan/version "1.0"
               :witan/inputs [{:witan/input-src-key :number}]}
              {:witan/name :mulx
               :witan/fn :witan.workspace-executor.function-catalog/mulX
               :witan/version "1.0"
               :witan/params {:x 3}
               :witan/inputs [{:witan/input-src-key :number}]}])
