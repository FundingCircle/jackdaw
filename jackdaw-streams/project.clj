(defproject fundingcircle/jackdaw-streams "0.1.0-SNAPSHOT"
  :description "Kafka streams clojure wrapper"
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[org.apache.kafka/kafka-streams "_"]]
  :profiles {:dev {:dependencies
                   [[clojure-future-spec "_"]
                    [org.apache.kafka/kafka-clients "_" :classifier "test"]
                    [org.apache.kafka/kafka-streams "_" :classifier "test"]]}})
