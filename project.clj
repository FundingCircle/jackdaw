(defproject fundingcircle/kafka.client "0.4.0-SNAPSHOT"
  :description "No frills Clojure wrapper around core kafka APIs"
  :url "http://github.com/FundingCircle/kafka.client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clojurewerkz/propertied "1.2.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.101tec/zkclient "0.8" :exclusions [log4j org.slf4j/slf4j-log4j12]]
                 [org.apache.kafka/kafka_2.11 "0.10.0.1" :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api
                                                                      com.fasterxml.jackson.core/jackson-annotations
                                                                      com.fasterxml.jackson.core/jackson-core]]
                 [org.apache.kafka/kafka-clients "0.10.0.1" :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api
                                                                         com.fasterxml.jackson.core/jackson-annotations
                                                                         com.fasterxml.jackson.core/jackson-core]]]

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v" "--no-sign"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]

  :repositories
  {"snapshots" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-snapshot-local"
                :username [:gpg :env/artifactory_user]
                :password [:gpg :env/artifactory_password]
                :sign-releases false}
   "releases" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-release-local"
               :username [:gpg :env/artifactory_user]
               :password [:gpg :env/artifactory_password]
               :sign-releases false}})
