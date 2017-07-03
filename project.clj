(defproject fundingcircle/jackdaw "0.1.0-SNAPSHOT"
  :description "No frills Clojure wrapper around Apache Kafka APIs"
  :url "http://github.com/FundingCircle/jackdaw"
  :dependencies [[org.apache.kafka/kafka-clients "0.10.2.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]
                 [org.apache.kafka/kafka_2.11 "0.10.2.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]]
  :plugins [[lein-sub "0.3.0"]]
  :sub ["jackdaw-client"
        "jackdaw-serdes"
        "jackdaw-streams"])
