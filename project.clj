(defproject fundingcircle/jackdaw "0.1.0-SNAPSHOT"
  :description "No frills Clojure wrapper around Apache Kafka APIs"
  :url "http://github.com/FundingCircle/jackdaw"
  :dependencies [[fundingcircle/jackdaw-client "0.1.0-SNAPSHOT"]
                 [fundingcircle/jackdaw-serdes "0.1.0-SNAPSHOT"]
                 [fundingcircle/jackdaw-streams "0.1.0-SNAPSHOT"]
                 [org.clojure/clojure "1.8.0"]]
  :plugins [[lein-codox "0.10.3"]
            [lein-sub "0.3.0"]]
  :sub ["jackdaw-client"
        "jackdaw-serdes"
        "jackdaw-streams"]
  :codox {:output-path "codox"
          :source-uri "http://github.com/fundingcircle/jackdaw/blob/{version}/{filepath}#L{line}"
          :source-paths ["jackdaw-client/src"
                         "jackdaw-serdes/src"
                         "jackdaw-streams/src"]}
  :source-paths ["jackdaw-client/src"
                 "jackdaw-serdes/src"
                 "jackdaw-streams/src"]
  :test-paths ["jackdaw-client/test"
               "jackdaw-serdes/test"
               "jackdaw-streams/test"]
  :profiles {:dev {:dependencies [[org.apache.kafka/kafka-clients "0.10.2.1" :classifier "test"]
                                  [org.apache.kafka/kafka-streams "0.10.2.1" :classifier "test"]
                                  [org.clojure/test.check "0.9.0"]]}}
  :test-selectors {:default (complement :integration)
                   :integration :integration})
