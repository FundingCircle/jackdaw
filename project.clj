(defproject fundingcircle/jackdaw "0.3.11-SNAPSHOT"
  :description "No frills Clojure wrapper around Apache Kafka APIs"

  :dependencies [[fundingcircle/jackdaw-admin "_"]
                 [fundingcircle/jackdaw-client "_"]
                 [fundingcircle/jackdaw-serdes "_"]
                 [fundingcircle/jackdaw-streams "_"]
                 [fundingcircle/jackdaw-test "_"]
                 [org.clojure/clojure "_"]]

  :plugins [[lein-codox "0.10.3"]
            [fundingcircle/lein-modules "0.3.13-SNAPSHOT"]]

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
                {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-snapshot-local"
                 :username [:gpg :env/artifactory_user]
                 :password [:gpg :env/artifactory_password]}]
               ["releases"
                {:url "https://fundingcircle.artifactoryonline.com/fundingcircle/libs-release-local"
                 :username [:gpg :env/artifactory_user]
                 :password [:gpg :env/artifactory_password]}]]

              :dependencies
              [[org.clojure/clojure "_"]]}

             ;; The dev profile - non-deployment configuration
             :dev
             {:test-selectors
              {:default (complement :integration)
               :integration :integration}

              :codox
              {:output-path "codox"
               :source-uri "http://github.com/fundingcircle/jackdaw/blob/{version}/{filepath}#L{line}"}

              :dependencies
              [[org.apache.kafka/kafka-clients "_" :classifier "test"]
               [org.apache.kafka/kafka-streams "_" :classifier "test"]]}

             :test
             {:dependencies [[org.clojure/test.check "_"]]}

             ;; This is not in fact what lein defines repl to be
             :repl
             [:default :dev :test]}

  :modules {;; Always inherit the shared configuration
            :inherited
            [:shared]

            ;; Shared pinned library versions
            :versions
            {fundingcircle :version
             io.confluent "4.0.0"
             junit "4.12"
             org.apache.kafka "1.0.1"
             org.clojure/clojure "1.9.0"
             org.clojure/test.check "0.9.0"
             org.clojure/tools.logging "0.3.1"
             org.clojure/data.json "0.2.6"
             org.clojure/tools.nrepl "0.2.12"
             org.clojure/java.jdbc "0.7.0-beta2"
             org.xerial/sqlite-jdbc "3.19.3"
             com.taoensso/nippy "2.12.2"
             danlentz/clj-uuid "0.1.7"
             environ "1.1.0"
             clj-time "0.13.0"
             clj-http "2.3.0"}}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["modules" "change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ; ["vcs" "tag"]
                  ["modules" "deploy"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["modules" "change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
