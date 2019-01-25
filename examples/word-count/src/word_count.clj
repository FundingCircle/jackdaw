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
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse])
  (:import [org.apache.kafka.common.serialization Serdes]))


;;; Topic Configuration
;;;

(defn topic-config
  "Takes a topic name and (optionally) a key and value serde and
  returns a topic configuration map, which may be used to create a
  topic or produce/consume records."
  ([topic-name]
   (topic-config topic-name (jse/serde) (jse/serde)))

  ([topic-name key-serde value-serde]
   {:topic-name topic-name
    :partition-count 1
    :replication-factor 1
    :key-serde key-serde
    :value-serde value-serde}))

(defn topic-names
  []
  ["input" "output"])


;;; App Template
;;;

(defn app-config
  "Returns the application config."
  []
  {"application.id" "word-count"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  "Returns a topology builder.

  WARNING: This is just a stub. Before publishing to the input topic,
  evaluate one of the `build-topology` functions in the comment forms
  below."
  [builder]
  builder)

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "word-count is up")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "word-count is down"))

(defn -main
  [& _]
  (start-app (app-config)))


(comment
  ;;; Start
  ;;;

  ;; Needed to invoke the forms from this namespace. When typing
  ;; directly in the REPL, skip this step.
  (require '[user :refer :all :exclude [topic-config]])


  ;; Start ZooKeeper and Kafka.
  ;; This requires the Confluent Platform CLI which may be obtained
  ;; from `https://www.confluent.io/download/`. If ZooKeeper and Kafka
  ;; are already running, skip this step.
  (confluent/start)


  ;; Create the `input` and `output` topics, and start the app.
  (start)


  ;; Get a list of current topics.
  (list-topics)


  )


(comment
  ;;; Example: Pipe
  ;;;
  ;;; Reads from a Kafka topic called `input`, logs the key and value,
  ;;; and writes these to a Kafka topic called `output`.
  ;;;
  ;;; This topology reads and writes using `jackdaw.serdes.edn/serde`,
  ;;; and logs using `jackdaw.streams/peek` which wraps
  ;;; `KStream#peek`.

  (defn build-topology
    [builder]
    (-> (j/kstream builder (topic-config "input"))
        (j/peek (fn [[k v]]
                  (info (str {:key k :value v}))))
        (j/to (topic-config "output")))
    builder)


  (reset)


  (publish (topic-config "input") nil "this is a pipe")


  (get-keyvals (topic-config "output"))


  (reset)


  (get-keyvals (topic-config "output"))


  )


(comment
  ;;; Example: Word Count
  ;;;
  ;;; Create business logic using pure functions

  (defn split-lines
    [input-string]
    (str/split (str/lower-case input-string) #"\W+"))


  (defn counts
    [input-string]
    (->> input-string
         split-lines
         (group-by identity)
         (reduce (fn [counts [word occurrences]]
                      (assoc counts word (count occurrences)))
                 {})))


  (counts (str/join " " ["all streams lead to kafka"
                         "hello kafka streams"]))


  )


(comment
  ;;; Example: Word Count
  ;;;
  ;;; Reads from a Kafka topic called `input`, logs the key and value,
  ;;; writes the counts to a topic called `output`.
  ;;;
  ;;; This topology uses a KTable to track how many times words are
  ;;; seen. The KTable is created by the combined use of
  ;;; `KStream#groupBy` and `KGroupedStream#count`.

  (defn build-topology
    [builder]
    (let [text-input (j/kstream builder (topic-config "input"))

          counts (-> text-input
                     (j/flat-map-values split-lines)
                     (j/group-by (fn [[_ v]] v)
                                 (topic-config nil (Serdes/String)
                                               (Serdes/String)))
                     (j/count))]

      (-> counts
          (j/to-kstream)
          (j/to (topic-config "output")))

      builder))


  (reset)


  (publish (topic-config "input") nil "all streams lead to kafka")


  (get-keyvals (topic-config "output"))


  (publish (topic-config "input") nil "hello kafka streams")


  (get-keyvals (topic-config "output"))


  (reverse (sort-by second (into {} (get-keyvals (topic-config "output")))))


  (list-topics)


  (get-keyvals (topic-config (str "word-count-KSTREAM-AGGREGATE-"
                                  "STATE-STORE-0000000003-changelog")
                             (Serdes/String)
                             (Serdes/Long)))


  (reset)


  (let [text-input (slurp (io/resource "metamorphosis.txt"))
        values (str/split text-input #"\n")]

    (doseq [v values]
      (publish (topic-config "input") nil v)
      (println v))
    (println "THE END"))

  (reverse (sort-by second (into {} (get-keyvals (topic-config "output")))))


  )
