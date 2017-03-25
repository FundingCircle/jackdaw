(defproject fundingcircle/kafka.serdes "0.11.1-SNAPSHOT"
  :description "Serializers/deserializers for Kafka"
  :url "https://github.com/FundingCircle/kafka-serdes"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[danlentz/clj-uuid "0.1.6"]
                 [environ "1.1.0"]
                 [io.confluent/kafka-avro-serializer "3.1.0"]
                 [io.confluent/kafka-schema-registry-client "3.1.0"
                  :exclusions [org.slf4j/slf4j-log4j12
                               org.slf4j/slf4j-api
                               com.fasterxml.jackson.core/jackson-databind]]
                 [org.apache.kafka/kafka-clients "0.10.1.0-cp2"
                  :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [com.taoensso/nippy "2.12.2"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]
                                  [com.fasterxml.jackson.core/jackson-databind "2.7.0"]]}
             :uberjar {:aot :all}}
  :test-selectors {:default (complement :integration)
                   :integration :integration}
  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v" "--no-sign"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]
  :repositories  {"confluent" {:url "http://packages.confluent.io/maven/"}
                  "snapshots" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-snapshot-local"
                               :username [:gpg :env/artifactory_user]
                               :password [:gpg :env/artifactory_password]
                               :sign-releases false}
                  "releases" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-release-local"
                              :username [:gpg :env/artifactory_user]
                              :password [:gpg :env/artifactory_password]
                              :sign-releases false}})
