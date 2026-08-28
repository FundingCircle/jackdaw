(ns test-runner
  "Runs the example tests after AOT-compiling jackdaw's gen-class serdes.

  jackdaw is consumed here as a source dependency (:local/root), so the
  gen-class jackdaw.serdes.EdnSerde referenced by jackdaw.streams.mock is not
  precompiled. We compile it (and expose it via the thread context classloader)
  before delegating to the Cognitect test runner."
  (:require [clojure.java.io :as io])
  (:import [java.net URLClassLoader]))

(defn -main [& args]
  (let [dir (io/file "classes")]
    (.mkdirs dir)
    (binding [*compile-path* "classes"]
      (run! compile '[jackdaw.serdes.edn2
                      jackdaw.serdes.fressian
                      jackdaw.serdes.fn-impl]))
    (let [thread (Thread/currentThread)
          cl (URLClassLoader. (into-array java.net.URL [(.toURL (.toURI dir))])
                              (.getContextClassLoader thread))]
      (.setContextClassLoader thread cl)))
  (require 'cognitect.test-runner)
  (apply (resolve 'cognitect.test-runner/-main) args))
