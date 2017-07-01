(defproject fundingcircle/jackdaw-client "0.1.0-SNAPSHOT"
  :description "Clojure wrapper for Apache Kafka Producer and Consumer APIs"
  :url "http://github.com/FundingCircle/jackdaw"
  :dependencies [[clojurewerkz/propertied "1.2.0"]
                 [org.apache.kafka/kafka-clients "0.10.2.1"
                  :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]
                 [org.apache.kafka/kafka_2.11 "0.10.2.1"
                  :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]]
  :repositories [["releases" {:url ~(str "https://fundingcircle.artifactoryonline.com"
                                         "/fundingcircle/libs-release-local")
                              :username [:gpg :env/artifactory_user]
                              :password [:gpg :env/artifactory_password]
                              :sign-releases false}]
                 ["snapshots" {:url ~(str "https://fundingcircle.artifactoryonline.com"
                                          "/fundingcircle/libs-snapshot-local")
                               :username [:gpg :env/artifactory_user]
                               :password [:gpg :env/artifactory_password]
                               :sign-releases false}]])
