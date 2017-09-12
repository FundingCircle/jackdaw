(defproject fundingcircle/jackdaw-client "0.1.0-SNAPSHOT"
  :description "Clojure wrapper for Apache Kafka Producer and Consumer APIs"
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[clojurewerkz/propertied "1.2.0"]
                 [org.apache.kafka/kafka-clients "_"]
                 [org.apache.kafka/kafka_2.11 "_"]])
