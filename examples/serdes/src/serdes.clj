(ns serdes
  "This tutorial contains a simple stream processing application using
  Jackdaw and Kafka Streams.

  It begins with pipe using EDN serdes and then shows how to do the
  same using Avro. The third example makers mixed serdes where the
  value is Avro and the key is a string."
  (:gen-class)
  (:require [clojure.algo.generic.functor :refer [fmap]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j]
            [jackdaw.serdes]
            [jackdaw.serdes.avro.confluent :as jsac]
            [jackdaw.serdes.edn :as jse]
            [jackdaw.serdes.resolver :as resolver]))


;;; Topic Configuration
;;;

(def +topic-metadata+
  {"input"
   {:topic-name "input"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-qualified-keyword :jackdaw.serdes.edn/serde}
    :value-serde {:serde-qualified-keyword :jackdaw.serdes.edn/serde}}

   "output"
   {:topic-name "output"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-qualified-keyword :jackdaw.serdes.edn/serde}
    :value-serde {:serde-qualified-keyword :jackdaw.serdes.edn/serde}}})

(def topic-metadata
  (memoize (fn []
             (fmap #(assoc % :key-serde ((resolver/serde-resolver) (:key-serde %))
                           :value-serde ((resolver/serde-resolver) (:value-serde %)))
                   +topic-metadata+))))


;;; App Template
;;;

(defn app-config
  "Returns the application config."
  []
  {"application.id" "serdes"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  "Returns a topology builder.

  Reads from a Kafka topic called `input`, logs the key and value, and
  writes these to a Kafka topic called `output`."
  [builder]
  (-> (j/kstream builder (get (topic-metadata) "input"))
      (j/peek (fn [[k v]]
                (info (str {:key k :value v}))))
      (j/to (get (topic-metadata) "output")))
  builder)

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "serdes is up")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "serdes is down"))

(defn -main
  [& _]
  (start-app (app-config)))


(comment
  ;;; Example: EDN Serdes
  ;;;
  ;;; Uses jackdaw.serdes.edn/serde.


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


  ;; Write to the input stream.
  (publish (get (topic-metadata) "input") "the-key" "the-value")


  ;; Read from the output stream.
  (get-keyvals (get (topic-metadata) "output"))


  )


(comment
  ;;; Example: Avro Serdes
  ;;;
  ;;; Uses jackdaw.serdes.avro/serde for the key and value.


  ;; Evaluate the forms.

  (def +topic-metadata+
    {"input"
     {:topic-name "input"
      :partition-count 1
      :replication-factor 1
      :key-serde {:serde-qualified-keyword :jackdaw.serdes.avro.confluent/serde
                  :schema-filename "key-schema.json"
                  :key? true}
      :value-serde {:serde-qualified-keyword :jackdaw.serdes.avro.confluent/serde
                    :schema-filename "value-schema.json"
                    :key? false}}

     "output"
     {:topic-name "output"
      :partition-count 1
      :replication-factor 1
      :key-serde {:serde-qualified-keyword :jackdaw.serdes.avro.confluent/serde
                  :schema-filename "key-schema.json"
                  :key? true}
      :value-serde {:serde-qualified-keyword :jackdaw.serdes.avro.confluent/serde
                    :schema-filename "value-schema.json"
                    :key? false}}})

  (def serde-resolver
    (partial resolver/serde-resolver :schema-registry-url "http://localhost:8081"))

  (def topic-metadata
    (memoize (fn []
               (fmap #(assoc % :key-serde ((serde-resolver) (:key-serde %))
                             :value-serde ((serde-resolver) (:value-serde %)))
                     +topic-metadata+))))


  ;; Recreate the `input` and `output` topics, and restart the app.
  (reset)


  ;; Publish a record to `input`.
  (publish (get (topic-metadata) "input")
           {:the-key "the-key"}
           {:the-value "the-value"})


  ;; Consume from `output`.
  (get-keyvals (get (topic-metadata) "output"))


  )


(comment
  ;;; Example: Mixed Serdes
  ;;;
  ;;; Uses jackdaw.serdes.avro.confluent/serde for the value and
  ;;; org.apache.kafka.common.serialization.Serdes for the key.


  ;; Evaluate the forms.

  (def +topic-metadata+
    {"input"
     {:topic-name "input"
      :partition-count 1
      :replication-factor 1
      :key-serde {:serde-qualified-keyword :jackdaw.serdes/string-serde}
      :value-serde {:serde-qualified-keyword :jackdaw.serdes.avro.confluent/serde
                    :schema-filename "value-schema.json"
                    :key? false}}

     "output"
     {:topic-name "output"
      :partition-count 1
      :replication-factor 1
      :key-serde {:serde-qualified-keyword :jackdaw.serdes/string-serde}
      :value-serde {:serde-qualified-keyword :jackdaw.serdes.avro.confluent/serde
                    :schema-filename "value-schema.json"
                    :key? false}}})

  (def serde-resolver
    (partial resolver/serde-resolver :schema-registry-url "http://localhost:8081"))

  (def topic-metadata
    (memoize (fn []
               (fmap #(assoc % :key-serde ((serde-resolver) (:key-serde %))
                             :value-serde ((serde-resolver) (:value-serde %)))
                     +topic-metadata+))))


  ;; Recreate the `input` and `output` topics, and restart the app.
  (reset)


  ;; Publish a record to `input`.
  (publish (get (topic-metadata) "input") "the-key" {:the-value "the-value"})


  ;; Consume from `output`.
  (get-keyvals (get (topic-metadata) "output"))


  )
