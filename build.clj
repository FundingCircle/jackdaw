(ns build
  "Build script replacing the old Leiningen jar/pom/deploy tasks.

  Usage:
    clojure -T:build jar     ;; write pom + AOT + build the jar
    clojure -T:build deploy  ;; build (if needed) and deploy to Clojars

  The version is taken from the JACKDAW_VERSION environment variable when set,
  otherwise it is derived from the most recent semver git tag (mirroring the
  behaviour of the old lein-git-version plugin)."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
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
  "Return the release version when HEAD is exactly a semver tag. When HEAD is a
  `publish-snapshot-<semver>` tag, return that semver as a SNAPSHOT. Otherwise
  return a SNAPSHOT version based on the next patch of the latest semver tag."
  []
  (or (System/getenv "JACKDAW_VERSION")
      (let [latest-tag (git "describe" "--tags" "--abbrev=0"
                            "--match" "[0-9]*.[0-9]*.[0-9]*")
            exact-tag  (git "describe" "--tags" "--exact-match"
                            "--match" "[0-9]*.[0-9]*.[0-9]*")
            snapshot-tag (git "describe" "--tags" "--exact-match"
                              "--match" "publish-snapshot-[0-9]*.[0-9]*.[0-9]*")]
        (cond
          exact-tag exact-tag

          snapshot-tag
          (let [[_ semver] (re-find #"publish-snapshot-(\d+\.\d+\.\d+)" snapshot-tag)]
            (format "%s-SNAPSHOT" semver))

          latest-tag
          (let [[_ prefix patch] (re-find #"(\d+\.\d+)\.(\d+)" latest-tag)]
            (format "%s.%d-SNAPSHOT" prefix (inc (Long/parseLong patch))))

          :else "0.0.0-SNAPSHOT"))))

(def version (derive-version))
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn- basis []
  ;; :root nil keeps the CLI's own Clojure out of the generated pom; deps.edn
  ;; declares central/clojars/confluent/mulesoft explicitly so resolution does
  ;; not rely on the root deps.edn's repos.
  (b/create-basis {:root nil :project "deps.edn"}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn- alias-dep-version
  "Read a dep's :mvn/version out of one of deps.edn's :aliases, so a version
  declared only in an alias has a single source of truth."
  [alias-key lib]
  (-> "deps.edn" slurp edn/read-string
      (get-in [:aliases alias-key :extra-deps lib :mvn/version])))

;; aleph deliberately isn't in :deps (see :test alias) so :local/root/:git/url
;; consumers don't inherit it. write-pom only publishes :deps though, and has
;; no concept of scope, so add it to the pom by hand.
(defn- add-test-scope-dep! [pom-file coord]
  (let [[group artifact] (str/split (str coord) #"/")
        version (or (alias-dep-version :test coord)
                    (throw (ex-info (str "No :mvn/version for " coord " in :test alias")
                                     {:coord coord})))
        dep-xml (format (str "    <dependency>\n"
                             "      <groupId>%s</groupId>\n"
                             "      <artifactId>%s</artifactId>\n"
                             "      <version>%s</version>\n"
                             "      <scope>test</scope>\n"
                             "    </dependency>\n")
                        group artifact version)
        content (slurp pom-file)
        patched (str/replace-first content #"<dependencies>" (str "<dependencies>\n" dep-xml))]
    (when (= content patched)
      (throw (ex-info "Could not find <dependencies> to insert into"
                       {:pom-file pom-file})))
    (spit pom-file patched)))

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
    (add-test-scope-dep! (b/pom-path {:class-dir class-dir :lib lib}) 'aleph/aleph)
    (b/copy-dir {:src-dirs ["src" "resources"]
                 :target-dir class-dir})
    (b/compile-clj {:basis basis
                    :src-dirs ["src"]
                    :class-dir class-dir
                    :ns-compile aot-nses})
    (b/jar {:class-dir class-dir
            :jar-file jar-file}))
  (println "Built" jar-file))

(defn- sign-key-id
  "Return the GPG key fingerprint used to sign artifacts.

  Read from GPG_KEY_ID so rotating the key needs no code change. A full
  fingerprint rather than the email uid, so gpg selects the signing key
  directly instead of a uid lookup, which reports a misleading \"no default
  secret key\" when the key is merely expired.

  Throws when unset or blank. gpg ignores an empty --default-key and signs
  with whatever secret key it finds, so a blank value would silently produce
  an artifact signed by the wrong key."
  []
  (or (some-> (System/getenv "GPG_KEY_ID") str/trim not-empty)
      (throw (ex-info "GPG_KEY_ID is not set; refusing to sign" {}))))

(defn deploy
  "Build the jar (if necessary) and deploy it to Clojars.

  Credentials are read from the CLOJARS_USERNAME / CLOJARS_PASSWORD environment
  variables. All releases, including snapshots, are GPG signed."
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
                :sign-key-id (sign-key-id)
                :sign-releases? true}))
  (println "Deployed" jar-file "to Clojars"))
