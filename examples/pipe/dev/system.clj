(ns system
  "Functions to start and stop the system, used for interactive
  development.
  The `system/start` and `system/stop` functions are required by the
  `user` namespace and should not be called directly."
  (:require [jackdaw.admin :as ja]
            [jackdaw.client :as jc]
            [pipe]
            [clojure.edn :as edn])
  (:import (java.util UUID)))

(defonce system (atom nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Configs ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn kafka-admin-client-config
  []
  {"bootstrap.servers" "localhost:9092"})

(def producer-config
  (assoc pipe/app-config "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                         "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"))

(defn consumer-config []
  (assoc pipe/app-config
    "group.id" (str (UUID/randomUUID))
    "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
    "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Additional setup ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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


(defn start-producer []
  (jc/producer producer-config))

(defn stop-producer [producer]
  (.close producer))

(defn start-consumer []
  (-> (consumer-config)
      (jc/consumer)
      (jc/subscribe [(pipe/topic-config "output")])
      (jc/seek-to-beginning-eager)))

(defn stop-consumer [consumer]
  (.close consumer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Utility functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn list-topics []
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/list-topics client)))

(defn publish [v]
  @(jc/produce! (:producer @system) (pipe/topic-config "input") (str v)))

(defn consume []
  (some->> (jc/poll (:consumer @system) 1000)
           (map (comp edn/read-string :value))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Start/Stop the system ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn stop
  "Stops the app and deletes topics."
  []
  (when @system
    (stop-producer (:producer @system))
    (stop-consumer (:consumer @system))
    (pipe/stop-app (:app @system)))
  (re-delete-topics #"(input|output)"))

(defn start
  "Creates topics, and starts the app."
  []
  (with-out-str (stop))
  (create-topics (map pipe/topic-config ["input" "output"]))
  (reset! system {:app      (pipe/start-app)
                  :producer (start-producer)
                  :consumer (start-consumer)})
  @system)

(defn reset []
  (stop)
  (start))
