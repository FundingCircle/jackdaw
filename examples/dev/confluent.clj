(ns confluent
  "doc-string"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]))

;; TODO: Explain this doesn't stop Kafka, etc. if the REPL is killed.

(defn not-up
  "doc-string"
  [service]
  (->> (:out (sh "confluent" "status"))
       str/split-lines
       (keep (fn [x] (re-find (re-pattern (str service " is.*")) x)))
       first
       (re-find #"DOWN")
       boolean))

(defn stop
  "doc-string"
  []
  (sh "confluent" "destroy")
  (log/info "kafka is down")
  (log/info "zookeeper is down"))

(defn start
  "doc-string"
  []
  (with-out-str (stop))
  (doseq [s ["zookeeper" "kafka"]]
    (do (while (not-up s)
          (sh "confluent" "start" s)
          (Thread/sleep 1000))
        (log/info s "is up"))))

(defn reset
  "doc-string"
  []
  (stop)
  (start))
