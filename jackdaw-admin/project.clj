(defproject fundingcircle/jackdaw-admin "0.3.14"
  :description "Tools for kafka administration"

  :dependencies [[org.clojure/tools.logging "_"]
                 [org.apache.kafka/kafka_2.11 "_"]
                 [environ "_"]]

  :plugins [[fundingcircle/lein-modules "[0.3.0,0.4.0)"]
            [lein-environ "1.1.0"]]

  :profiles {:kafka
             {:dependencies [[fundingcircle/jackdaw-test "_"]
                             [fundingcircle/jackdaw-client "_"]]}

             :dev [:kafka]

             :test [{:resource-paths ["test/resources"]} :kafka]})
