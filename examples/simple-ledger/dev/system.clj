(ns system
  "Functions to start and stop the system, used for interactive
  development.
  The `system/start` and `system/stop` functions are required by the
  `user` namespace and should not be called directly."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.admin :as ja]
            [simple-ledger]))

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

(defn application-id
  "Takes an application config and returns an `application.id`."
  [app-config]
  (get app-config "application.id"))

(defn destroy-state-stores
  "Takes an application config and deletes local files associated with
  internal state."
  [app-config]
  (sh "rm" "-rf" (str "/tmp/kafka-streams/"
                      (application-id app-config)))
  (log/info "internal state is deleted"))

(defn stop
  "Stops the app, and deletes topics and internal state.
  This functions is required by the `user` namespace and should not
  be called directly."
  []
  (when system
    (simple-ledger/stop-app (:app system)))
  (let [app-id (application-id (simple-ledger/app-config))
        re (re-pattern (str "(ledger-entries-requested"
                            "|ledger-transaction-added"
                            "|" (application-id
                                 (simple-ledger/app-config))
                            ".*)"))]
    (re-delete-topics re)
    (destroy-state-stores (simple-ledger/app-config))))

(defn start
  "Creates topics, and starts the app.
  This functions is required by the `user` namespace and should not
  be called directly."
  []
  (with-out-str (stop))
  (create-topics (map word-count/topic-config ["ledger-entries-requested"
                                               "ledger-transaction-added"]))
  {:app (simple-ledger/start-app (simple-ledger/app-config))})
