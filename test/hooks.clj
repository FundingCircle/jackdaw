(ns hooks)

(defn start-load-message [config]
  (println "About to start loading!")
  (clojure.pprint/pprint config)
  config)