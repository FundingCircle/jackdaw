(ns hooks.jackdaw.test.transports
  "clj-kondo hook for jackdaw.test.transports/deftransport.

  deftransport expands to a defmethod, so linting it as defn (which expects a
  simple symbol name) reports a false error on the keyword dispatch value. This
  hook rewrites the call to a fn so the argument vector and body are analysed
  correctly."
  (:require [clj-kondo.hooks-api :as api]))

(defn deftransport [{:keys [node]}]
  (let [[_ _transport-type args & body] (:children node)
        new-node (api/list-node
                  (list* (api/token-node 'clojure.core/fn)
                         args
                         body))]
    {:node (with-meta new-node (meta node))}))
