(defproject fundingcircle/jackdaw "_"
  :description "A Clojure library for the Apache Kafka distributed streaming platform."

  :scm {:name "git" :url "https://github.com/fundingcircle/jackdaw"}

  :url "https://github.com/FundingCircle/jackdaw/"

  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]]

  :aot [jackdaw.serdes.fn-impl jackdaw.serdes.edn]
  :plugins [[me.arrdem/lein-git-version "2.0.8"]
            [lein-tools-deps "0.4.3"]]

  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}

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
