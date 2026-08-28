(ns test-runner
  "Runs the test suite via kaocha, but first AOT-compiles the gen-class serdes.

  Leiningen used to AOT these namespaces (project.clj :aot). Under the deps.edn
  CLI there is no implicit AOT step, so we compile them here into `classes/`
  before loading any tests. Without this, Kafka cannot instantiate
  jackdaw.serdes.EdnSerde et al. by class name (e.g. the default.key.serde
  config used by jackdaw.streams.mock).

  The system classloader will not pick up a `classes` directory that did not
  exist at JVM startup, so we also expose the freshly compiled classes via the
  thread context classloader, which is what Kafka uses to resolve serde
  classes."
  (:require [clojure.java.io :as io])
  (:import [java.net URLClassLoader]))

(def ^:private aot-nses
  '[jackdaw.serdes.edn2
    jackdaw.serdes.fressian
    jackdaw.serdes.fn-impl])

(defn -main [& args]
  (let [dir (io/file "classes")]
    (.mkdirs dir)
    (binding [*compile-path* (str dir)]
      (run! compile aot-nses))
    (let [thread (Thread/currentThread)
          cl (URLClassLoader. (into-array java.net.URL [(.toURL (.toURI dir))])
                              (.getContextClassLoader thread))]
      (.setContextClassLoader thread cl)))
  (require 'kaocha.runner)
  (apply (resolve 'kaocha.runner/-main) args))
