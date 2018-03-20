(defproject fundingcircle/jackdaw-client "0.3.11-SNAPSHOT"
  :description "Clojure wrapper for Apache Kafka Producer and Consumer APIs"

  :plugins [[fundingcircle/lein-modules "0.3.13-SNAPSHOT"]]

  :dependencies [[org.apache.kafka/kafka-clients "_"]
                 [org.apache.kafka/kafka_2.11 "_"]])
