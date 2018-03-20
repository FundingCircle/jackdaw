(defproject fundingcircle/jackdaw-admin "0.3.11-SNAPSHOT"
  :description "Tools for kafka administration"

  :dependencies [[org.clojure/tools.logging "_"]
                 [org.apache.kafka/kafka_2.11 "_"]
                 [environ "_"]]

  :plugins [[fundingcircle/lein-modules "0.3.13-SNAPSHOT"]
            [lein-environ "1.1.0"]]

  :profiles {:kafka
             {:dependencies [[fundingcircle/jackdaw-test "_"]
                             [fundingcircle/jackdaw-client "_"]]}

             :dev
             [{:env {:zookeeper-address "127.0.0.1:2181"
                     :bootstrap-servers "127.0.0.1:9092"}}
              :kafka]

             :test
             [{:resource-paths ["test/resources"]
               :env {;; confluent packages have ports xxx9 to prevent conflict with local env
                     :zookeeper-address "127.0.0.1:2189"
                     :bootstrap-servers "127.0.0.1:9099"}}
              :kafka]})
