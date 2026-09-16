(defproject streams "0.1.0-SNAPSHOT"
  :description "streams-examples"

  :target-path "target/%s"
  :uberjar-name "streams-examples.jar"
  :main streams.main

  :url "https://github.com/FundingCircle/jackdaw"

  :dependencies [[org.clojure/clojure "1.10.3"]

                 [danlentz/clj-uuid "0.1.9"]
                 [org.clojure/tools.logging "1.1.0"]
                 [ch.qos.logback/logback-classic "1.2.7"]

                 ;; Kafka Helpers
                 [fundingcircle/topology-grapher "0.1.3"
                  :exclusions [org.clojure/data.priority-map]]

                 ;; Kafka
                 ;; Explicitly bring in Kafka, rather than transitively
                 [org.apache.kafka/kafka-clients "2.8.1"]
                 [org.apache.kafka/kafka-streams "2.8.1"]
                 [fundingcircle/jackdaw "0.9.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-databind
                               com.thoughtworks.paranamer/paranamer
                               joda-time
                               org.apache.kafka/kafka-clients
                               org.apache.kafka/kafka-streams
                               org.apache.zookeeper/zookeeper
                               org.slf4j/slf4j-log4j12]]]

  :resource-paths ["resources" "resources/avro-schemas"]

  :repl-options {:init-ns streams.main}

  :profiles
  {:dev {:resource-paths ["test/resources"]
         :dependencies [[org.apache.kafka/kafka-streams-test-utils "2.8.1"]
                        [org.apache.kafka/kafka_2.13 "2.8.1"]]}}

  :repositories
  {"confluent" {:url "https://packages.confluent.io/maven/"}})
