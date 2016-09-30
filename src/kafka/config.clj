(ns kafka.config
  "Build java.util.Properties from Clojure maps"
  (:import
   (java.util Properties)))

(defn properties
  "Generate java.util.Properties for a clojure map

   If a `path` is supplied, generate properties only for the value
   obtained by invoking `(get-in m path)`."
  ([m]
   (props m []))

  ([m path]
   (let [props (Properties. )]
     (doseq [[n v] (get-in m path)]
       (.setProperty props n v))
     props)))
