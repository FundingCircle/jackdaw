(defproject fundingcircle/jackdaw-streams "0.1.0-SNAPSHOT"
  :description "Kafka streams clojure wrapper"
  :url "https://github.com/FundingCircle/kstreams-common"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.apache.kafka/kafka-streams "0.10.2.1"]
                 [org.clojure/clojure "1.8.0"]]
  :profiles {:dev {:dependencies
                   [[org.apache.kafka/kafka-clients "0.10.2.1" :classifier "test"]
                    [org.apache.kafka/kafka-streams "0.10.2.1" :classifier "test"]]}})
