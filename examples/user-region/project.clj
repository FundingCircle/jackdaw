(defproject user-region "0.1.0-SNAPSHOT"
  :description "Demonstrates group-by operations and aggregations on KTable by computing
  the number of users per region"
  :dependencies [[fundingcircle/jackdaw "0.4.3"]
                 [org.apache.kafka/kafka-streams "2.0.0"]
                 [org.apache.kafka/kafka-streams-test-utils "2.0.0"]
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.1"]]
  :repositories [["confluent" "https://packages.confluent.io/maven/"]]
  :source-paths ["src" "dev" "../dev"]
  :main ^:skip-aot user-region.core
  :target-path "target/%s")
