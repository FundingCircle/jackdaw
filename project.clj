(defproject fundingcircle/jackdaw "_"
  :description "A Clojure library for the Apache Kafka distributed streaming platform."

  :scm {:name "git" :url "https://github.com/fundingcircle/jackdaw"}

  :url "https://github.com/FundingCircle/jackdaw/"

  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]]

  :dependencies [[aleph "0.4.6"]
                 [clj-time "0.15.1"]
                 [danlentz/clj-uuid "0.1.7"]
                 ;; Confluent does paired releases with Kafka, this should tie
                 ;; off with the kafka version.
                 ;; See https://docs.confluent.io/current/release-notes.html

                 [io.confluent/kafka-schema-registry-client "5.1.0"]
                 [io.confluent/kafka-avro-serializer "5.1.0"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [org.apache.kafka/kafka-streams "2.1.0"]
                 [org.apache.kafka/kafka_2.11 "2.1.0"]
                 [org.clojure/clojure "1.9.0" :scope "provided"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/core.cache "0.7.2"]]

  :aot [jackdaw.serdes.fn-impl]
  :plugins [[me.arrdem/lein-git-version "2.0.8"]]

  :git-version
  {:status-to-version
   (fn [{:keys [tag version branch ahead ahead? dirty?] :as git}]
     (if (and tag (not ahead?) (not dirty?))
       tag
       (let [[_ prefix patch] (re-find #"(\d+\.\d+)\.(\d+)" tag)
             patch            (Long/parseLong patch)
             patch+           (inc patch)]
         (format "%s.%d-%s-SNAPSHOT" prefix patch+ branch))))}

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
                "https://packages.confluent.io/maven/"]]

              :deploy-repositories
              [["clojars" {:url "https://clojars.org/repo/"
                           :username :env/clojars_username
                           :password :env/clojars_password
                           :signing {:gpg-key "fundingcirclebot@fundingcircle.com"}}]]}

             ;; The dev profile - non-deployment configuration
             :dev
             {:source-paths
              ["dev"]

              :injections [(require 'io.aviso.logging.setup)]
              :dependencies [[io.aviso/logging "0.3.1"]
                             [org.apache.kafka/kafka-streams-test-utils "2.1.0"]
                             [org.apache.kafka/kafka-clients "2.1.0" :classifier "test"]
                             [org.clojure/test.check "0.9.0"]]}

             :test
             {:resource-paths ["test/resources"]
              :plugins [[lein-cloverage "1.0.13"]]}

             ;; This is not in fact what lein defines repl to be
             :repl
             [:default :dev :test]})
