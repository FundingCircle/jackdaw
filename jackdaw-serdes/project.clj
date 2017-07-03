(defproject fundingcircle/jackdaw-serdes "0.1.0-SNAPSHOT"
  :description "Serializers/deserializers for Kafka"
  :url "https://github.com/FundingCircle/jackdaw"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[danlentz/clj-uuid "0.1.6"]
                 [environ "1.1.0"]
                 [io.confluent/kafka-avro-serializer "3.2.1"]
                 [io.confluent/kafka-schema-registry-client "3.2.1"]
                 [org.apache.kafka/kafka-clients "0.10.2.1"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [com.taoensso/nippy "2.12.2"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]]}}
  :test-selectors {:default (complement :integration)
                   :integration :integration}
  :repositories  {"confluent" {:url "http://packages.confluent.io/maven/"}})
