(ns confluent
  "Functions to start and stop ZooKeeper and Kafka.
  These functions require the Confluent Platform CLI which can be
  obtained from `https://www.confluent.io/download/`.

  WARNING: Quitting the REPL will not stop ZooKeeper and Kafka. Before
  exiting, you must invoke `confluent/stop`. Otherwise, run `confluent
  destroy` from the command line."
  (:require [clojure.string :as str]
            [clojure.java.shell :refer [sh]]))

(defn not-up
  "Takes `service` and returns true if the service is down"
  [service]
  (->> (:out (sh "confluent" "status"))
       str/split-lines
       (keep (fn [x] (re-find (re-pattern (str service " is.*")) x)))
       first
       (re-find #"DOWN")
       boolean))

(defn stop
  "Stops ZooKeeper and Kafka."
  []
  (sh "confluent" "destroy")
  (println "kafka is down")
  (println "zookeeper is down"))

(defn start
  "Starts ZooKeeper and Kafka."
  []
  (with-out-str (stop))
  (doseq [s ["zookeeper" "kafka"]]
    (do (while (not-up s)
          (sh "confluent" "start" s)
          (Thread/sleep 1000))
        (println s "is up"))))

(defn reset
  "Stops and starts ZooKeeper and Kafka."
  []
  (stop)
  (start))
