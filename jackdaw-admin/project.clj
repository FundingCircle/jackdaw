(defproject fundingcircle/jackdaw-admin "0.1.0-SNAPSHOT"
  :description "Tools for kafka administration"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :main kafka.admin.core
  :target-path "target/%s"
  :uberjar-name "kafka.admin.jar"

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.kafka/kafka_2.11 "0.10.2.1"]
                 [environ "1.1.0"
                  :exclusions [org.clojure/clojure]]
                 [boot-environ "1.1.0"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.cli "0.3.5"]
                 [reply "0.3.7"
                  :exclusions [commons-io
                               commons-codec]]]
  :plugins [[lein-environ "1.1.0"]
            [lein-marginalia "0.9.0"]]

  :repl-options {:welcome (prn "Welcome to Kafka administration tool. See `kafka.admin.topic` namespace for more information")
                 :caught  clj-stacktrace.repl/pst+
                 :init-ns jackdaw.user}

  :profiles {:dev          [:project/dev :kafka]
             :test         [:project/test :kafka]
             :project/test {:resource-paths ["test/resources"]
                            :env            {;; confluent packages have ports xxx9 to prevent conflict with local env
                                             :zookeeper-address "127.0.0.1:2189"
                                             :bootstrap-servers "127.0.0.1:9099"}}
             :project/dev  {:env {:zookeeper-address "127.0.0.1:2181"
                                  :bootstrap-servers "127.0.0.1:9092"}}

             :kafka        {:dependencies [[fundingcircle/kafka.test "0.3.2"
                                            :exclusions [[org.apache.kafka/kafka-clients
                                                          org.slf4j/slf4j-log4j12
                                                          com.fasterxml.jackson.core/jackson-core]]]
                                           [fundingcircle/kafka.client "0.6.3"
                                            :exclusions [org.apache.kafka/kafka-clients
                                                         org.slf4j/slf4j-log4j12]]]}}

  :repositories [["confluent"  {:url "http://packages.confluent.io/maven/"}]
                 ["releases"
                  {:url           "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-release-local"
                   :sign-releases false
                   :username      [:gpg :env/artifactory_user]
                   :password      [:gpg :env/artifactory_password]}]
                 ["snapshots"
                  {:url           "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-snapshot-local"
                   :sign-releases false
                   :username      [:gpg :env/artifactory_user]
                   :password      [:gpg :env/artifactory_password]}]]

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v" "--no-sign"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
