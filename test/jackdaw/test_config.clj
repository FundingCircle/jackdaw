(ns jackdaw.test-config
  (:require
   [aero.core :as aero]
   [clojure.java.io :as io]))

(defn test-config
  ([]
   (test-config "config.edn"))
  ([config]
   (aero/read-config (io/resource config))))

