(ns system
  "Functions to start and stop the system, used for interactive
  development.
  The `system/start` and `system/stop` functions are required by the
  `user` namespace and should not be called directly."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.admin.client :as j.admin.client]
            [pipe]))

(def system nil)

(defn create-topics
  "Takes a list of topics and creates these using the names given."
  [topic-names]
  (let [topic-config-list (map pipe/topic-config topic-names)]
    (with-open [client (j.admin.client/client {"bootstrap.servers"
                                               "localhost:9092"})]
      (j.admin.client/create-topics client topic-config-list))))

(defn re-delete-topics
  "Takes an instance of java.util.regex.Pattern and deletes any Kafka
  topics that match."
  [re]
  (with-open [client (j.admin.client/client {"bootstrap.servers"
                                             "localhost:9092"})]
    (let [topics-to-delete (->> (j.admin.client/get-topics client)
                                (filter #(re-find re %))
                                (map pipe/topic-config))]
      (j.admin.client/delete-topics client topics-to-delete))))

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
  (create-topics ["input" "output"])
  {:app (pipe/start-app (pipe/app-config))})
