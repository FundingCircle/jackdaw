(defproject fundingcircle/jackdaw "0.1.0-SNAPSHOT"
  :description "No frills Clojure wrapper around Apache Kafka APIs"
  :url "http://github.com/FundingCircle/jackdaw"
  :license {:name "3-Clause BSD License",
            :url "https://opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[fundingcircle/jackdaw-admin "0.1.0-SNAPSHOT"]
                 [fundingcircle/jackdaw-client "0.1.0-SNAPSHOT"]
                 [fundingcircle/jackdaw-serdes "0.1.0-SNAPSHOT"]
                 [fundingcircle/jackdaw-streams "0.1.0-SNAPSHOT"]
                 [fundingcircle/jackdaw-test "0.1.0-SNAPSHOP"]
                 [org.clojure/clojure "1.8.0"]]
  :plugins [[lein-codox "0.10.3"]
            [lein-modules "0.3.11"]]
  :codox {:output-path "codox"
          :source-uri "http://github.com/fundingcircle/jackdaw/blob/{version}/{filepath}#L{line}"}
  :source-paths ["jackdaw-admin/src"
                 "jackdaw-client/src"
                 "jackdaw-serdes/src"
                 "jackdaw-streams/src"]
  :profiles {:dev {:dependencies [[org.apache.kafka/kafka-clients "_" :classifier "test"]
                                  [org.apache.kafka/kafka-streams "_" :classifier "test"]
                                  [org.clojure/test.check "_"]]}
             :provided {:dependencies [[org.clojure/clojure "_"]]}}
  :modules {:inherited {:repositories {"confluent" {:url "http://packages.confluent.io/maven/"}}
                        :url "https://github.com/FundingCircle/jackdaw"
                        :license {:name "BSD 3-clause"
                                  :url "http://opensource.org/licenses/BSD-3-Clause"}}
            :versions {clojure-future-spec "1.9.0-alpha17"
                       io.confluent/kafka-connect-avro-converter "3.2.1"
                       io.confluent/kafka-connect-jdbc "3.2.1"
                       io.confluent/kafka-schema-registry "3.2.1"
                       io.confluent/kafka-avro-serializer "3.2.1"
                       io.confluent/kafka-schema-registry-client "3.2.1"
                       org.apache.kafka/connect-api "0.10.2.1"
                       org.apache.kafka/connect-json "0.10.2.1"
                       org.apache.kafka/connect-runtime "0.10.2.1"
                       org.apache.kafka/kafka_2.11 "0.10.2.1"
                       org.apache.kafka/kafka-clients "0.10.2.1"
                       org.apache.kafka/kafka-streams "0.10.2.1"
                       org.clojure/clojure "1.8.0"
                       org.clojure/test.check "0.9.0"
                       org.clojure/tools.logging "0.3.1"}}
  :test-selectors {:default (complement :integration)
                   :integration :integration})
