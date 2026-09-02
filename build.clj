(ns build
  "Build script replacing the old Leiningen jar/pom/deploy tasks.

  Usage:
    clojure -T:build jar     ;; write pom + AOT + build the jar
    clojure -T:build deploy  ;; build (if needed) and deploy to Clojars

  The version is taken from the JACKDAW_VERSION environment variable when set,
  otherwise it is derived from the most recent semver git tag (mirroring the
  behaviour of the old lein-git-version plugin)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]
            [deps-deploy.gpg]))

(def lib 'fundingcircle/jackdaw)

;; Namespaces that were AOT compiled by the old project.clj :aot vector.
(def aot-nses
  '[jackdaw.serdes.edn2
    jackdaw.serdes.fressian
    jackdaw.serdes.fn-impl])

(defn- git [& args]
  (try
    (not-empty (str/trim (b/git-process {:git-args (str/join " " args)})))
    (catch Exception _ nil)))

(defn- derive-version
  "Return the release version when HEAD is exactly a semver tag, otherwise a
  SNAPSHOT version based on the next patch of the latest semver tag."
  []
  (or (System/getenv "JACKDAW_VERSION")
      (let [latest-tag (git "describe" "--tags" "--abbrev=0"
                            "--match" "[0-9]*.[0-9]*.[0-9]*")
            exact-tag  (git "describe" "--tags" "--exact-match"
                            "--match" "[0-9]*.[0-9]*.[0-9]*")]
        (cond
          exact-tag exact-tag

          latest-tag
          (let [[_ prefix patch] (re-find #"(\d+\.\d+)\.(\d+)" latest-tag)]
            (format "%s.%d-SNAPSHOT" prefix (inc (Long/parseLong patch))))

          :else "0.0.0-SNAPSHOT"))))

(def version (derive-version))
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def snapshot? (str/ends-with? version "-SNAPSHOT"))

(defn- basis []
  ;; :root nil keeps deps at their deps.edn scope in the pom (mirrors the old
  ;; lein :scope "provided") rather than pinning to the build container's basis.
  (b/create-basis {:root nil :project "deps.edn"}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar
  "Write the pom, AOT compile the serdes namespaces and build the jar."
  [_]
  (clean nil)
  (let [basis (basis)]
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :scm {:url "https://github.com/fundingcircle/jackdaw"
                        :connection "scm:git:git://github.com/fundingcircle/jackdaw.git"
                        :developerConnection "scm:git:ssh://git@github.com/fundingcircle/jackdaw.git"}
                  :pom-data
                  [[:description "A Clojure library for the Apache Kafka distributed streaming platform."]
                   [:url "https://github.com/FundingCircle/jackdaw/"]
                   [:licenses
                    [:license
                     [:name "BSD 3-clause"]
                     [:url "http://opensource.org/licenses/BSD-3-Clause"]]]]})
    (b/copy-dir {:src-dirs ["src" "resources"]
                 :target-dir class-dir})
    (b/compile-clj {:basis basis
                    :src-dirs ["src"]
                    :class-dir class-dir
                    :ns-compile aot-nses})
    (b/jar {:class-dir class-dir
            :jar-file jar-file}))
  (println "Built" jar-file))

(defn deploy
  "Build the jar (if necessary) and deploy it to Clojars.

  Credentials are read from the CLOJARS_USERNAME / CLOJARS_PASSWORD environment
  variables. Non-snapshot releases are GPG signed."
  [_]
  (when-not (.exists (io/file jar-file))
    (jar nil))
  ;; deps-deploy 0.2.5 calls gpg/read-passphrase unconditionally in sign!, even
  ;; when a key id is supplied. System/console is nil on a non-TTY, so this NPEs
  ;; in CI. bin/gpg supplies the passphrase via loopback pinentry, so the value
  ;; read here is never used.
  (with-redefs [deps-deploy.gpg/read-passphrase (constantly "")]
    (dd/deploy {:installer :remote
                :artifact jar-file
                :pom-file (b/pom-path {:class-dir class-dir :lib lib})
                :sign-key-id "fundingcirclebot@fundingcircle.com"
                :sign-releases? (not snapshot?)}))
  (println "Deployed" jar-file "to Clojars"))
