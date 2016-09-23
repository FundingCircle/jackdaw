(defproject kstreams-common "0.1.0-SNAPSHOT"
  :description "Kafka streams clojure wrapper"
  :url "https://github.com/FundingCircle/kstreams-common"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :local-repo ".repo"
  :dependencies [[org.apache.kafka/kafka-streams "0.10.0.0-cp1"]
                 [org.clojure/clojure "1.8.0"]]
  :repositories {"confluent" {:url "http://packages.confluent.io/maven/"}
                 "snapshots" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-snapshot-local"
                              :username [:gpg :env/artifactory_user]
                              :password [:gpg :env/artifactory_password]
                              :sign-releases false}
                 "releases" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-release-local"
                             :username [:gpg :env/artifactory_user]
                             :password [:gpg :env/artifactory_password]
                             :sign-releases false}})
