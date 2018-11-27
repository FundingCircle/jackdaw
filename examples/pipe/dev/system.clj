(ns system
  "Functions to start and stop the system, used for interactive
  development.
  The `system/start` and `system/stop` functions are required by the
  `user` namespace and should not be called directly."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.admin :as ja]
            [pipe]))

(def system nil)

(defn kafka-admin-client-config
  []
  {"bootstrap.servers" "localhost:9092"})

(defn create-topics
  "Takes a list of topics and creates these using the names given."
  [topic-config-list]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/create-topics! client topic-config-list)))

(defn re-delete-topics
  "Takes an instance of java.util.regex.Pattern and deletes any Kafka
  topics that match."
  [re]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (let [topics-to-delete (->> (ja/list-topics client)
                                (filter #(re-find re (:topic-name %))))]
      (ja/delete-topics! client topics-to-delete))))

(defn stop
  "Stops the app and deletes topics.
  This functions is required by the `user` namespace and should not
  be called directly."
  []
  (when system
    (pipe/stop-app (:app system)))
  (re-delete-topics #"(input|output)"))

(defn start
  "Creates topics, and starts the app.
  This functions is required by the `user` namespace and should not
  be called directly."
  []
  (with-out-str (stop))
  (create-topics (map pipe/topic-config ["input" "output"]))
  {:app (pipe/start-app (pipe/app-config))})
