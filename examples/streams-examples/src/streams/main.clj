(ns streams.main
  (:require
   [clojure.tools.logging :as log]
   [streams.stack-calculator :as stack-calculator]
   [streams.core :as core]))

(defn execute!
  [topology-config-fn stream-build-fn action-fn]
  (let [stream-config (-> (core/load-config)
                          (assoc :hard-exit-on-error true)
                          (topology-config-fn)
                          (core/reify-serdes-config))]
    (action-fn stream-build-fn stream-config)))

(defn render-basic-stream-dsl [topology-name]
  (case topology-name
    "stack-calculator"
    (execute! stack-calculator/configure-topology
              stack-calculator/build-stream
              core/render-topology)
    (str "Unknown topology " topology-name)))
  
(defn -main [& args]
  (log/info {:args args} "Running application ...")
  (let [base-config (assoc (core/load-config)
                           :hard-exit-on-error true)]
    (doseq [topology-name args]
      (log/info {:topology-name topology-name} "Starting topology")
      (case topology-name
        "stack-calculator"
        (execute! stack-calculator/configure-topology
                  stack-calculator/build-stream
                  core/run-topology)
        (throw (ex-info (str "Unknown topology " topology-name) {:args args}))))))
