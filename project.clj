(defproject fundingcircle/jackdaw "_"
  :description "A Clojure library for the Apache Kafka distributed streaming platform."

  :scm {:name "git" :url "https://github.com/fundingcircle/jackdaw"}

  :url "https://github.com/FundingCircle/jackdaw/"

  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]
                 ["mulesoft" {:url "https://repository.mulesoft.org/nexus/content/repositories/public/"}]]

  :dependencies [[aleph/aleph "0.4.7"]
                 [danlentz/clj-uuid "0.1.9"
                  :exclusions [primitive-math]]

                 ;; Confluent does paired releases with Kafka, this should tie
                 ;; off with the kafka version.
                 ;; See https://docs.confluent.io/current/release-notes.html
                 [io.confluent/kafka-schema-registry-client "6.1.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                 [io.confluent/kafka-avro-serializer "6.1.1"]
                 [io.confluent/kafka-json-schema-serializer "6.1.1"]
                 [org.apache.kafka/kafka-clients "2.8.0"]
                 [org.apache.kafka/kafka-streams "2.8.0"]
                 [org.apache.kafka/kafka-streams-test-utils "2.8.0"]

                 [org.clojure/clojure "1.10.1" :scope "provided"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/data.fressian "1.0.0"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.cache "1.0.225"]
                 [metosin/jsonista "0.3.6"]

                 ;; Use specific commons-compress version to avoid
                 ;; CVE-2021-36090 pulled by avro 1.9.2
                 [org.apache.commons/commons-compress "1.21"]]

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
                             [org.apache.kafka/kafka-streams-test-utils "2.8.0"]
                             [org.apache.kafka/kafka-clients "2.8.0" :classifier "test"]
                             [org.clojure/test.check "1.1.1"]
                             [org.apache.kafka/kafka_2.13 "2.8.0"]
                             [lambdaisland/kaocha "1.66.1034"]
                             [lambdaisland/kaocha-cloverage "1.0.75"]
                             [lambdaisland/kaocha-junit-xml "0.0.76"]]}

             ;; This is not in fact what lein defines repl to be
             :repl
             [:default :dev]})
