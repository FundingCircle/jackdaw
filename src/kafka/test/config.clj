(ns kafka.test.config
  (:require
   [clojure.string :as str]
   [kafka.test.fs :as fs]
   [environ.core :refer [env]])
  (:import
   (java.util Properties)))

(defn host-port [host-str]
  (try
   (let [[host port] (str/split host-str #"\:")]
      {:host host
       :port (Integer/parseInt port)})
    (catch Exception e
      (throw (ex-info "invalid host string: " {:host-str host-str} e)))))

(defn properties
  "Generate java.util.Properties for a clojure map
   If a `path` is supplied, generate properties only for the value
   obtained by invoking `(get-in m path)`."
  ([m]
   (properties m []))

  ([m path]
   (let [props (Properties. )]
     (doseq [[n v] (get-in m path)]
       (.setProperty props n v))
     props)))

(defn multi-config
  "Convenience function for generating multi-config functions compatible with the
   multi-broker fixture

   Pass in a base config and this will convert it into a multi-config by making node
   specific updates to the following keys

    broker.id
    port
    advertised.port
    log.dirs
  "
  [base]
  (fn [n]
    (let [port (+ n (Integer/parseInt (or (get base "port")
                                          "9092")))]
      (assoc base
             "broker.id" (str n)
             "port" (str port)
             "advertised.port" (str port)
             "log.dirs" (str (get base "log.dirs")
                             "-"
                             n)))))
