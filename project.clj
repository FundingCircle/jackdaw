(defproject fundingcircle/jackdaw "0.4.0-SNAPSHOT"
  :description "A Clojure library for the Apache Kafka distributed streaming platform."

  :dependencies [[clj-time "0.13.0"]
                 [com.taoensso/nippy "2.12.2"]
                 [danlentz/clj-uuid "0.1.7"]
                 [environ "1.1.0"]
                 ;; Confluent does paired releases with Kafka, this should tie
                 ;; off with the kafka version.
                 ;; See https://docs.confluent.io/current/release-notes.html
                 [io.confluent/kafka-avro-serializer "5.0.0"]
                 [io.confluent/kafka-connect-avro-converter "5.0.0"]
                 [io.confluent/kafka-connect-jdbc "5.0.0"]
                 [io.confluent/kafka-schema-registry "5.0.0"]
                 [io.confluent/kafka-schema-registry-client "5.0.0"]
                 [org.apache.kafka/connect-api "2.0.0"]
                 [org.apache.kafka/connect-json "2.0.0"]
                 [org.apache.kafka/connect-runtime "2.0.0"]
                 [org.apache.kafka/kafka-clients "2.0.0"]
                 [org.apache.kafka/kafka-streams "2.0.0"]
                 [org.apache.kafka/kafka_2.11 "2.0.0"]
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.cache "0.7.1"]]

  :plugins [[lein-codox "0.10.3"]
            [lein-environ "1.1.0"]]

  :aot [jackdaw.serdes.fn-impl]

  :profiles {;; Provide an alternative to :leiningen/default, used to include :shared
             :default
             [:base :system :user :shared :provided :dev]

             ;; Define a profile intended to be shared by this project and its children
             :shared
             {:url "https://github.com/FundingCircle/jackdaw"
              :license {:name "BSD 3-clause"
                        :url "http://opensource.org/licenses/BSD-3-Clause"}

              :repositories
              [["confluent"
                "https://packages.confluent.io/maven/"]
               ["clojars"
                "https://clojars.org/repo/"]
               ["snapshots"
                {:url "https://fundingcircle.jfrog.io/fundingcircle/libs-snapshot-local"
                 :username [:gpg :env/artifactory_user]
                 :password [:gpg :env/artifactory_password]}]
               ["releases"
                {:url "https://fundingcircle.jfrog.io/fundingcircle/libs-release-local"
                 :username [:gpg :env/artifactory_user]
                 :password [:gpg :env/artifactory_password]}]]}

             ;; The dev profile - non-deployment configuration
             :dev
             {:source-paths
              ["dev"]

              :codox
              {:output-path "codox"
               :source-uri "http://github.com/fundingcircle/jackdaw/blob/{version}/{filepath}#L{line}"}}

             :test
             {:resource-paths ["test/resources"]
              :dependencies   [[arohner/wait-for "1.0.2"]
                               [clj-http "2.3.0"]
                               [environ "1.1.0"]
                               [junit "4.12"]
                               [org.apache.kafka/kafka-streams-test-utils "2.0.0" :scope "test"]
                               [org.clojure/java.jdbc "0.7.0-beta2"]
                               [org.clojure/test.check "0.9.0"]
                               [org.clojure/tools.nrepl "0.2.12"]
                               [org.xerial/sqlite-jdbc "3.19.3"]]}

             ;; This is not in fact what lein defines repl to be
             :repl
             [:default :dev :test]})
