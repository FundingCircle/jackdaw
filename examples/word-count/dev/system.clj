(ns system
  "doc-string"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.admin.client :as j.admin.client]
            [word-count]))

(def system nil)

(defn not-up
  "doc-string"
  [service]
  (->> (:out (sh "confluent" "status"))
       str/split-lines
       (keep (fn [x] (re-find (re-pattern (str service " is.*")) x)))
       first
       (re-find #"DOWN")
       boolean))

(defn start-confluent-platform
  "doc-string"
  [service-list]
  (doseq [s service-list]
    (do (while (not-up s)
          (sh "confluent" "start" s)
          (Thread/sleep 1000))
        (log/info s "is up"))))

(defn create-topics
  "doc-string"
  [topic-names]
  (let [topic-config-list (map word-count/topic-config topic-names)]
    (with-open [client (j.admin.client/client {"bootstrap.servers" "localhost:9092"})]
      (j.admin.client/create-topics client topic-config-list))))

(defn destroy-confluent-platform
  "doc-string"
  []
  (sh "confluent" "destroy")
  (log/info "kafka is down")
  (log/info "zookeeper is down"))

(defn destroy-state-stores
  "doc-string"
  [application-id]
  (sh "rm" "-rf" (str "/tmp/kafka-streams/" application-id))
  (log/info "internal state is deleted"))

(defn stop
  []
  "doc-string"
  (when system
    (word-count/stop-topology (:topology system)))
  (destroy-state-stores (get (word-count/topology-config) "application.id"))
  (destroy-confluent-platform))

(defn start
  []
  "doc-string"
  (with-out-str (stop))
  (start-confluent-platform ["zookeeper" "kafka"])
  (create-topics ["input" "output"])
  {:topology (word-count/start-topology word-count/topology-config)})
