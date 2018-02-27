(defproject fundingcircle/jackdaw-admin "0.3.10-SNAPSHOT"
  :description "Tools for kafka administration"
  :dependencies [[org.clojure/tools.logging "_"]
                 [org.apache.kafka/kafka_2.11 "_"]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev [:project/dev :kafka]
             :test [:project/test :kafka]
             :project/test {:resource-paths ["test/resources"]
                            :env {;; confluent packages have ports xxx9 to prevent conflict with local env
                                  :zookeeper-address "127.0.0.1:2189"
                                  :bootstrap-servers "127.0.0.1:9099"}
                            :plugins [[lein-environ "1.1.0"]]
                            :dependencies [[environ "1.1.0" :exclusions [org.clojure/clojure]]]}
             :project/dev {:env {:zookeeper-address "127.0.0.1:2181"
                                 :bootstrap-servers "127.0.0.1:9092"}}
             :kafka {:dependencies [[fundingcircle/jackdaw-test "_"]
                                    [fundingcircle/jackdaw-client "_"]]}})
