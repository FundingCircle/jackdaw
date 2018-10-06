(defproject fundingcircle/jackdaw-test "0.3.25"
  :description "Test fixtures for Kafka, Zookeeper, and Confluent Schema Registry"

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :plugins [[fundingcircle/lein-modules "[0.3.0,0.4.0)"]]

  :dependencies [[io.confluent/kafka-connect-avro-converter "_"]
                 [io.confluent/kafka-connect-jdbc "_"]
                 [io.confluent/kafka-schema-registry "_"]
                 [org.apache.kafka/connect-api "_"]
                 [org.apache.kafka/connect-json "_"]
                 [org.apache.kafka/connect-runtime "_"]
                 [org.clojure/tools.logging "_"]]
  :profiles {:test {:resource-paths ["test/resources"]
                    :dependencies   [[arohner/wait-for "_"]
                                     [clj-http "_"]
                                     [clj-time "_"]
                                     [environ "_"]
                                     [fundingcircle/jackdaw-client "_"]
                                     [org.clojure/data.json "_"]
                                     [org.clojure/tools.nrepl "_"]
                                     [org.clojure/java.jdbc "_"]
                                     [org.xerial/sqlite-jdbc "_"]]}})
