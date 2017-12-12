(defproject fundingcircle/jackdaw-serdes "0.3.1"
  :description "Serializers/deserializers for Kafka"
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[danlentz/clj-uuid "0.1.7"]
                 [environ "1.1.0"]
                 [io.confluent/kafka-avro-serializer "_"]
                 [io.confluent/kafka-schema-registry-client "_"]
                 [org.apache.kafka/kafka-clients "_"]
                 [org.clojure/data.json "0.2.6"]
                 [com.taoensso/nippy "2.12.2"]]
  :aot [jackdaw.serdes.avro
        jackdaw.serdes.edn
        jackdaw.serdes.json
        jackdaw.serdes.uuid]
  :profiles {:dev {:dependencies [[org.clojure/test.check "_"]]}}
  :test-selectors {:default (complement :integration)
                   :integration :integration})
