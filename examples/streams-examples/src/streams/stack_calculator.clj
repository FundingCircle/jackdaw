(ns streams.stack-calculator
  (:require
    [clojure.tools.logging :as log]
    [jackdaw.streams :as js]))

(def application-id "basic-stream-dsl")

(def op-map {"+" +
             "-" -
             "*" *
             "/" /})

(defn- apply-opfn [op [a b & vl]]
  (concat (list (op b a)) vl))

(defn stack-reducer [acc [_ v]]
  (if-let [opfn (op-map v)]
    (apply-opfn opfn acc)
    (concat [v] acc)))

(defn stack-calculator-topology [builder
                                 {:keys [input
                                         output] :as topics}
                                 stores]
  (-> builder
      (js/kstream input)
      (js/peek (fn [m]
                 (log/info ::received m)))
      (js/group-by-key)
      (js/aggregate list
                    stack-reducer
                    output)
      (js/to-kstream)
      (js/peek (fn [m]
                 (log/info ::produced m)))
      (js/to output))
  builder)

(defn configure-topology [config]
  (assoc-in config
            [:streams-settings :application-id]
            application-id))

(defn build-stream
  ([config]
   (build-stream (js/streams-builder) config))
  ([builder {:keys [topics stores] :as config}]
   (stack-calculator-topology builder topics stores)))
