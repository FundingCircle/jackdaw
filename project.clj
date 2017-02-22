(defproject fundingcircle/kafka.streams "0.5.1-SNAPSHOT"
  :description "Kafka streams clojure wrapper"
  :url "https://github.com/FundingCircle/kstreams-common"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.apache.kafka/kafka-streams "0.10.1.0-cp2"]
                 [org.clojure/clojure "1.8.0"]]
  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v" "--no-sign"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]
  :profiles {:dev {:dependencies
                   [[org.apache.kafka/kafka-streams "0.10.1.0-cp2" :classifier "test"]]}}
  :repositories {"confluent" {:url "http://packages.confluent.io/maven/"}
                 "snapshots" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-snapshot-local"
                              :username [:gpg :env/artifactory_user]
                              :password [:gpg :env/artifactory_password]
                              :sign-releases false}
                 "releases" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-release-local"
                             :username [:gpg :env/artifactory_user]
                             :password [:gpg :env/artifactory_password]
                             :sign-releases false}})
