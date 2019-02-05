(defproject user-events "0.1.0-SNAPSHOT"
  :description "Demonstrates group-by operations and aggregations on KTable by computing
  user-count per region"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[fundingcircle/jackdaw "0.4.3"]
                 [org.apache.kafka/kafka-streams "2.0.0"]
                 [org.apache.kafka/kafka-streams-test-utils "2.0.0"]
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.1"]]
  :repositories [["confluent" "https://packages.confluent.io/maven/"]]
  :source-paths ["src" "test" "dev" "../dev"]
  :main ^:skip-aot user-events.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
