(ns hooks)

(defn start-load-message [testable test-plan]
  (println "testable")
  (clojure.pprint/pprint testable)
  (println "test-plan")
  (clojure.pprint/pprint test-plan)
  testable)