(defproject kstreams-common "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :local-repo ".repo"
  :dependencies [[org.apache.kafka/kafka-streams "0.10.0.0-cp1"]
                 [org.clojure/clojure "1.8.0"]]
  :repositories {"confluent"
                 {:url "http://packages.confluent.io/maven/"}})
