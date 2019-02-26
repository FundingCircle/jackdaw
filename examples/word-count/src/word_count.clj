(ns word-count
  "This tutorial contains a simple stream processing application using
  Jackdaw and Kafka Streams.

  It begins with an app template which is then extended through a
  series of examples to illustrate key concepts in Kafka Streams using
  an interactive workflow. The result is a simple word counter.

  This follows the treatment outlined in
  `https://kafka.apache.org/20/documentation/streams/tutorial`."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j])
  (:import [org.apache.kafka.common.serialization Serdes]))

;;; Streams Configuration
;;;

(def app-config
  {"application.id" "word-count"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"
   "default.key.serde" (-> (Serdes/String) .getClass .getName)
   "default.value.serde" (-> (Serdes/String) .getClass .getName)})

;;; Topic Configuration
;;;

(defn topic-config
  [topic-name key-serde value-serde]
   {:topic-name topic-name
    :partition-count 1
    :replication-factor 1
    :key-serde key-serde
    :value-serde value-serde})

(def input (topic-config "input" (Serdes/String) (Serdes/String)))
(def output (topic-config "output" (Serdes/String) (Serdes/Long)))

(def word-count-topics
  {:input input
   :output output})

(defn parse-line
  [line]
  (let [line (-> (.toLowerCase line (java.util.Locale/getDefault)))]
    (->> (.split line "\\W+")
         (into []))))

(defn word-count
  [{:keys [input output]}]
  (fn [builder]
    (let [counts (-> (j/kstream builder input)
                     (j/flat-map-values (fn [line]
                                          (parse-line line)))
                     (j/group-by (fn [[k v]]
                                   v))
                     (j/count)
                     (j/to-kstream))]
      (j/to counts output)
      builder)))

(defn -main
  [& _]
  (let [builder (j/streams-builder)
        build-fn (word-count word-count-topics)
        topology (build-fn builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "word-count is up")
    app))

