(ns witan.workspace-executor.schema
  (:require [schema.core :as s]
            [witan.workspace-api :refer [WorkflowFnMetaData
                                         WorkflowPredicateMetaData]]))

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

(def Contract
  (s/conditional
   :witan/predicate?
   WorkflowPredicateMetaData
   :else
   WorkflowFnMetaData))

(def CatalogEntry
  {:witan/name s/Keyword
   :witan/fn   s/Keyword
   :witan/version s/Str ;; TODO check semver
   (s/optional-key :witan/params) s/Any})

(def Workspace
  {:workflow  Workflow
   :contracts [Contract]
   :catalog   [CatalogEntry]})
