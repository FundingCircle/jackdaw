(defproject fundingcircle/kafka.client "0.1.2-SNAPSHOT"
  :description "No frills Clojure wrapper around core kafka APIs"
  :url "http://github.com/FundingCircle/kafka.client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.101tec/zkclient "0.8"]
                 [org.apache.kafka/kafka_2.11 "0.10.0.1"]
                 [org.apache.kafka/kafka-clients "0.10.0.1"]]
  :repositories
  {"snapshots" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-snapshot-local"
                :username [:gpg :env/artifactory_user]
                :password [:gpg :env/artifactory_password]
                :sign-releases false}
   "releases" {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-release-local"
               :username [:gpg :env/artifactory_user]
               :password [:gpg :env/artifactory_password]
               :sign-releases false}})
