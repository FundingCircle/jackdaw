(defproject fundingcircle/jackdaw-admin "0.1.0-SNAPSHOT"
  :description "Tools for kafka administration"
  :main kafka.admin.core
  :target-path "target/%s"
  :uberjar-name "kafka.admin.jar"
  :dependencies [[org.clojure/tools.logging "_"]
                 [org.apache.kafka/kafka_2.11 "_"]
                 [environ "1.1.0" :exclusions [org.clojure/clojure]]
                 [boot-environ "1.1.0" :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.cli "0.3.5"]
                 [reply "0.3.7" :exclusions [commons-io commons-codec]]]
  :plugins [[lein-environ "1.1.0"]
            [lein-marginalia "0.9.0"]
            [lein-modules "0.3.11"]]

  :repl-options {:welcome (prn "Welcome to Kafka administration tool. See `kafka.admin.topic` namespace for more information")
                 :caught clj-stacktrace.repl/pst+
                 :init-ns jackdaw.user}

  :profiles {:dev [:project/dev :kafka]
             :test [:project/test :kafka]
             :project/test {:resource-paths ["test/resources"]
                           :env {;; confluent packages have ports xxx9 to prevent conflict with local env
                                 :zookeeper-address "127.0.0.1:2189"
                                 :bootstrap-servers "127.0.0.1:9099"}}
             :project/dev {:env {:zookeeper-address "127.0.0.1:2181"
                                 :bootstrap-servers "127.0.0.1:9092"}}
             :kafka {:dependencies [[fundingcircle/jackdaw-test "0.1.0-SNAPSHOT"]
                                    [fundingcircle/jackdaw-client "0.1.0-SNAPSHOT"]]}})
