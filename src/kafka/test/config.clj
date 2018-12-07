(ns kafka.test.config
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import
   (java.util Properties)))

(defn props
  "Generate java.util.Properties for a clojure map

   If a `path` is supplied, generate properties only for the sub-tree
   rooted at `path`."
  ([m]
   (props m []))

  ([m path]
   (let [props (Properties. )]
     (doseq [[n v] (get-in m path)]
       (.setProperty props n v))
     props)))
