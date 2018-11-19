(ns word-count
  "doc-string"
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as j.s.edn])
  (:import [org.apache.kafka.common.serialization Serdes]))

(defn topic-config
  "doc-string"
  ([topic-name]
   (topic-config topic-name (j.s.edn/serde)))

  ([topic-name value-serde]
   (topic-config topic-name (j.s.edn/serde) value-serde))

  ([topic-name key-serde value-serde]
   (topic-config topic-name 1 key-serde value-serde))

  ([topic-name partitions key-serde value-serde]
   {:jackdaw.topic/topic-name topic-name
    :jackdaw.topic/partitions partitions
    :jackdaw.topic/replication-factor 1
    :jackdaw.serdes/key-serde key-serde
    :jackdaw.serdes/value-serde value-serde}))

(defn topology-config
  "doc-string"
  []
  {"application.id" "word-count"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn build-topology [builder]
  "doc-string"
  (let [text-input (-> (j/kstream builder (topic-config "input"))
                       (j/peek (fn [[k v]]
                                 (log/info (format "key=%s value=%s" k v)))))

        ;; comment
        counts (-> text-input
                   (j/flat-map-values (fn [v]
                                        (str/split (str/lower-case v) #"\s")))
                   (j/group-by (fn [[_ v]] v)
                               (topic-config nil (Serdes/String) (Serdes/String)))
                   (j/count))]

    ;; comment
    (-> counts
        (j/to-kstream)
        (j/to (topic-config "output")))

    builder))

(defn start-topology
  "doc-string"
  [topology-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology (topology-config))]
    (j/start app)
    (log/info "word-count is up")
    app))

(defn stop-topology
  "doc-string"
  [app]
  (j/close app)
  (log/info "word-count is down"))

(defn -main
  [& _]
  (start-topology (topology-config)))
