(defproject fundingcircle/jackdaw-streams "0.3.27-SNAPSHOT"
  :description "Kafka streams clojure wrapper"

  :plugins [[lein-modules "0.3.11"]]

  :dependencies [[org.apache.kafka/kafka-streams "_"]]

  :profiles {:dev {:dependencies
                   [[clj-time "_"]
                    [org.apache.kafka/kafka-clients "_" :classifier "test"]
                    [org.apache.kafka/kafka-streams "_" :classifier "test"]
                    [org.clojure/tools.logging "_"]
                    [junit "_"]]}})
