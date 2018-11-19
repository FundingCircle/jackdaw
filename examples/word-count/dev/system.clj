(ns system
  "doc-string"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.admin.client :as j.admin.client]
            [word-count]))

(def system nil)

(defn create-topics
  "doc-string"
  [topic-names]
  (let [topic-config-list (map word-count/topic-config topic-names)]
    (with-open [client (j.admin.client/client {"bootstrap.servers" "localhost:9092"})]
      (j.admin.client/create-topics client topic-config-list))))

(defn re-delete-topics
  "doc-string"
  [re]
  (with-open [client (j.admin.client/client {"bootstrap.servers" "localhost:9092"})]
    (let [topics-to-delete (->> (j.admin.client/get-topics client)
                                (filter #(re-find re %))
                                (map word-count/topic-config))]
      (j.admin.client/delete-topics client topics-to-delete))))

(defn destroy-state-stores
  "doc-string"
  [application-id]
  (sh "rm" "-rf" (str "/tmp/kafka-streams/" application-id))
  (log/info "internal state is deleted"))

(defn stop
  []
  "doc-string"
  (let [application-id (get (word-count/topology-config) "application.id")]
    (when system
      (word-count/stop-topology (:topology system)))
    (re-delete-topics (re-pattern (str "(input|output|" application-id ".*)")))
    (destroy-state-stores application-id)))

(defn start
  []
  "doc-string"
  (with-out-str (stop))
  (create-topics ["input" "output"])
  {:topology (word-count/start-topology word-count/topology-config)})
