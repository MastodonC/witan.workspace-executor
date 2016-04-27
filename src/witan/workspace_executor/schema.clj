(ns witan.workspace-executor.schema
  (:require [schema.core :as s]))

(defn even-count?
  [x]
  ((comp even? count) x))

(def WorkflowBranch
  [(s/one s/Keyword "pred")
   (s/one s/Keyword "exit")
   (s/one s/Keyword "loop")])

(def WorkflowNode
  [(s/one s/Keyword "from")
   (s/one
    (s/conditional 
     keyword? s/Keyword
     :else WorkflowBranch)
    "to")])

(def Workflow
  [WorkflowNode])

(def ContractOutput
  {:witan/schema       s/Any
   :witan/key          s/Keyword
   :witan/display-name s/Str})

(def ContractInput
  {:witan/schema       s/Any
   :witan/key          s/Keyword
   :witan/display-name s/Str})

(def Contract
  {:witan/impl s/Symbol
   :witan/fn   s/Keyword
   :witan/version s/Str ;; TODO check semver
   :witan/outputs [ContractOutput]
   (s/optional-key :witan/inputs) [ContractInput]
   (s/optional-key :witan/params-schema) (s/maybe s/Any)})

(def Input
  {:witan/input-src-key s/Any ;; in this context, 'key' could be a string (such as s3 key)
   (s/optional-key :witan/input-src-fn) s/Symbol
   (s/optional-key :witan/input-dest-key) s/Keyword})

(def Output
  {:witan/output-src-key s/Keyword
   :witan/output-dest-key s/Keyword})

(def CatalogEntry
  {:witan/name s/Keyword
   :witan/fn   s/Keyword
   :witan/version s/Str ;; TODO check semver
   :witan/inputs [Input]
   (s/optional-key :witan/outputs) [Output]
   (s/optional-key :witan/params) s/Any})

(def Workspace
  {:workflow  Workflow
   :contracts [Contract]
   :catalog   [CatalogEntry]})
