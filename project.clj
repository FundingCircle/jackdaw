(defproject fundingcircle/jackdaw "_"
  :description "A Clojure library for the Apache Kafka distributed streaming platform."

  :scm {:name "git" :url "https://github.com/fundingcircle/jackdaw"}

  :url "https://github.com/FundingCircle/jackdaw/"

  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]
                 ["mulesoft" {:url "https://repository.mulesoft.org/nexus/content/repositories/public/"}]]

  :dependencies [[aleph "0.6.1"]
                 [danlentz/clj-uuid "0.1.9"
                  :exclusions [primitive-math]]

                 ;; Confluent does paired releases with Kafka, this should tie
                 ;; off with the kafka version.
                 ;; See https://docs.confluent.io/current/release-notes.html
                 [io.confluent/kafka-schema-registry-client "7.3.2"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                 [io.confluent/kafka-avro-serializer "7.3.2"]
                 [io.confluent/kafka-json-schema-serializer "7.3.2"]
                 [org.apache.kafka/kafka-clients "3.2.0"]
                 [org.apache.kafka/kafka-streams "3.2.0"]
                 [org.apache.kafka/kafka-streams-test-utils "3.2.0"]

                 [org.clojure/clojure "1.11.1" :scope "provided"]
                 [org.clojure/java.data "1.0.95"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/data.fressian "1.0.0"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.cache "1.0.225"]
                 [metosin/jsonista "0.3.7"]]

  :aliases {"kaocha" ["run" "-m" "kaocha.runner"]}
  :aot [jackdaw.serdes.edn2 jackdaw.serdes.fressian jackdaw.serdes.fn-impl]
  :plugins [[me.arrdem/lein-git-version "2.0.8"]]

  :git-version
  {:status-to-version
   (fn [{:keys [tag version branch ahead ahead? dirty?] :as git}]
     (if (and tag (not ahead?) (not dirty?))
       tag
       (let [[_ prefix patch] (re-find #"(\d+\.\d+)\.(\d+)" tag)
             patch            (Long/parseLong patch)
             patch+           (inc patch)
             branch+          (-> branch
                                  (.replaceAll "[^a-zA-Z0-9]" "_"))]
         (format "%s.%d-%s-SNAPSHOT" prefix patch+ branch+))))}

  :profiles {;; Provide an alternative to :leiningen/default, used to include :shared
             :default
             [:base :system :user :shared :provided :dev]

             ;; Define a profile intended to be shared by this project and its children
             :shared
             {:url "https://github.com/FundingCircle/jackdaw"
              :license {:name "BSD 3-clause"
                        :url "http://opensource.org/licenses/BSD-3-Clause"}
              :repositories
              [["confluent" "https://packages.confluent.io/maven/"]]

              :deploy-repositories
              [["clojars" {:url "https://clojars.org/repo/"
                           :username :env/clojars_username
                           :password :env/clojars_password
                           :signing {:gpg-key "fundingcirclebot@fundingcircle.com"}}]]}

             ;; The dev profile - non-deployment configuration
             :dev
             {:source-paths
              ["dev"]

              :resource-paths ["test/resources"]
              :injections [(require 'io.aviso.logging.setup)]
              :dependencies [[io.aviso/logging "1.0"]
                             [org.apache.kafka/kafka-streams-test-utils "3.2.0"]
                             [org.apache.kafka/kafka-clients "3.2.0" :classifier "test"]
                             [org.clojure/test.check "1.1.1"]
                             [org.apache.kafka/kafka_2.13 "3.2.0"]
                             [lambdaisland/kaocha "1.80.1274"]
                             [lambdaisland/kaocha-cloverage "1.1.89"]
                             [lambdaisland/kaocha-junit-xml "1.17.101"]]}

             ;; This is not in fact what lein defines repl to be
             :repl
             [:default :dev]})
