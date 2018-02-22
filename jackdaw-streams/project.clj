(defproject fundingcircle/jackdaw-streams "0.3.8-SNAPSHOT"
  :description "Kafka streams clojure wrapper"
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[org.apache.kafka/kafka-streams "_"]]
  :profiles {:dev {:dependencies
                   [[org.apache.kafka/kafka-clients "_" :classifier "test"]
                    [org.apache.kafka/kafka-streams "_" :classifier "test"]]}})
