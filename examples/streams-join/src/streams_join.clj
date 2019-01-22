(ns streams-join.core
  "This tutorial contains a simple stream processing application using
  Jackdaw and Kafka Streams."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse])
  (:import [org.apache.kafka.common.serialization Serdes]))


(defn topic-config
  "Takes a topic name and returns a topic configuration map, which may
  be used to create a topic or produce/consume records."
  [topic-name]
  {:topic-name topic-name
   :partition-count 1
   :replication-factor 1
   :key-serde (jse/serde)
   :value-serde (jse/serde)})


(defn app-config
  "Returns the application config."
  []
  {"application.id" "investors-with-addresses"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  "Reads from a Kafka topics called `investors` and `addresses`,
  and writes these to a Kafka topic called `investors-with-addresses`.
  Returns a topology builder."
  [builder1 builder2]
  (-> (j/kstream builder1 (topic-config "investors"))
      (j/kstream builder2 (topic-config "addresses"))

      (j/join builder1 builder2 (fn [[b1 b2]]
                                  (= (get b1 :id) (get b2 :investor_id) )))

      (j/to (topic-config "investors-with-addresses")))
  nil) ;; What should I return here?

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder1 (j/streams-builder)
        builder2 (j/streams-builder)
        topology (build-topology builder1 builder2)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "joined stream is ready")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "Bye bye"))


(defn -main
  [& _]
  (start-app (app-config)))


(comment
  ;;;; Evaluate the forms.

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

  ;; Write to the input stream.
  (publish (topic-config "investors")
           nil
           {:id 1
            :name "Investor #001"})
  (publish (topic-config "investors")
           nil
           {:id 2
            :name "Investor #002"})
  (publish (topic-config "investors")
           nil
           {:id 3
            :name "Investor #003"})

  (publish (topic-config "addresses")
           nil
           {:id 1
            :investor_id 3
            :address "Oak Street, London"})
  (publish (topic-config "addresses")
           nil
           {:id 2
            :investor_id 2
            :address "Maple Close, London"})
  (publish (topic-config "addresses")
           nil
           {:id 3
            :investor_id 1
            :address "Pine Avenue"})

  ;; Read from the output stream.
  (get-keyvals (topic-config "investors-with-addresses"))
  )
