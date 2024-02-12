(ns hooks)

(defn start-load-message [config]
  (println "config")
  (clojure.pprint/pprint config)
  config)