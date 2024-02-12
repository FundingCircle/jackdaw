(ns hooks)

(defn start-load-message [testable config]
  (println "config")
  (clojure.pprint/pprint config)
  (println "testable")
  (clojure.pprint/pprint testable)
  testable)