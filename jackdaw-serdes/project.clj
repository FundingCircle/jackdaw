(defproject fundingcircle/jackdaw-serdes "0.3.22-SNAPSHOT"
  :description "Serializers/deserializers for Kafka"

  :plugins [[fundingcircle/lein-modules "[0.3.0,0.4.0)"]]

  :dependencies [[danlentz/clj-uuid "_"]
                 [environ "_"]
                 [io.confluent/kafka-avro-serializer "_"]
                 [io.confluent/kafka-schema-registry-client "_"]
                 [org.apache.kafka/kafka-clients "_"]
                 [org.clojure/clojure "_"]
                 [org.clojure/data.json "_"]
                 [org.clojure/tools.logging "_"]
                 [com.taoensso/nippy "_"]]

  :aot [jackdaw.serdes.fn])
