(defproject fundingcircle/jackdaw-test "0.1.0-SNAPSHOT"
  :description "Test fixtures for Kafka, Zookeeper, and Confluent Schema Registry"
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[io.confluent/kafka-connect-avro-converter "_"]
                 [io.confluent/kafka-connect-jdbc "_"]
                 [io.confluent/kafka-schema-registry "_"]
                 [org.apache.kafka/connect-api "_"]
                 [org.apache.kafka/connect-json "_"]
                 [org.apache.kafka/connect-runtime "_"]
                 [org.clojure/tools.logging "_"]]
  :profiles {:dev {:dependencies [[clj-http "2.3.0"]
                                  [clj-time "0.13.0"]
                                  [environ "1.1.0"]
                                  [org.clojure/data.json "0.2.6"]
                                  [org.clojure/tools.nrepl "0.2.12"]
                                  [fundingcircle/jackdaw-client "0.1.0-SNAPSHOT"]
                                  [org.clojure/java.jdbc "0.7.0-beta2"]
                                  [org.xerial/sqlite-jdbc "3.19.3"]]
                   :resource-paths ["test/resources"]}})