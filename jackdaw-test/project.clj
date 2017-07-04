(defproject fundingcircle/jackdaw-test "0.1.0-SNAPSHOT"
  :description "Test fixtures for Kafka, Zookeeper, and Confluent Schema Registry"
  :url "http://github.com/fundingcircle/jackdaw-test"
  :dependencies [[clojurewerkz/propertied "1.2.0"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.8.7"]
                 [com.fasterxml.jackson.core/jackson-core "2.8.7"]
                 [com.fasterxml.jackson.jaxrs/jackson-jaxrs-base "2.8.7"]
                 [com.fasterxml.jackson.jaxrs/jackson-jaxrs-json-provider "2.8.7"]
                 [io.confluent/kafka-connect-avro-converter "3.2.1"]
                 [io.confluent/kafka-connect-jdbc "3.2.1"]
                 [io.confluent/kafka-schema-registry "3.2.1"]
                 [org.apache.kafka/connect-api "0.10.2.1"]
                 [org.apache.kafka/connect-json "0.10.2.1"]
                 [org.apache.kafka/connect-runtime "0.10.2.1"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ragtime "0.6.3"]]
  ;; :aliases {"migrate" ["run" "-m" "kafka.test.config/migrate"]
  ;;           "rollback" ["run" "-m" "kafka.test.config/rollback"]}
  :profiles
  {:dev {:plugins [[lein-environ "1.1.0"]]
         :dependencies [[ch.qos.logback/logback-classic "1.1.1"]
                        [clj-http "2.3.0"]
                        [environ "1.1.0"]
                        [org.clojure/data.json "0.2.6"]
                        [org.clojure/tools.nrepl "0.2.12"]
                        [fundingcircle/jackdaw-client "0.1.0-SNAPSHOT"]
                        [org.postgresql/postgresql "9.4.1208.jre7"]
                        [org.slf4j/log4j-over-slf4j "1.7.21"]]
         ;; :resource-paths ["test/resources"]
         :env {:zookeeper-connect "localhost:2181"
               :bootstrap-servers "localhost:9092"
               :schema-registry-url "http://127.0.0.1:8081"
               :kafka-connect-host "localhost"
               :kafka-connect-port "8083"
               ;; :db-host "localhost"
               ;; :db-user "postgres"
               ;; :db-password ""
               ;; :db-name "kafka_test_development"
               ;; :db-port "5432"
               }}
   :test {:plugins [[lein-environ "1.1.0"]]
          :dependencies [[clj-time "0.13.0"]
                         [environ "1.1.0"]
                         [org.postgresql/postgresql "9.4.1208.jre7"]]
          :resource-paths ["test/resources"]
          ;; :env {:db-host "localhost"
          ;;       :db-user "postgres"
          ;;       :db-password ""
          ;;       :db-name "kafka_test_test"
          ;;       :db-port "5432"}
          }}
  :repositories {"confluent" {:url "http://packages.confluent.io/maven/"}})
