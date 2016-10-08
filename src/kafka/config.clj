(ns kafka.config
  "Build java.util.Properties from Clojure maps"
  (:import java.util.Properties))

(set! *warn-on-reflection* true)

(defn properties
  "Generate java.util.Properties for a clojure map

   If a `path` is supplied, generate properties only for the value
   obtained by invoking `(get-in m path)`."
  ^java.util.Properties
  ([m]
   (properties m []))

  ([m path]
   (let [props (Properties. )]
     (doseq [[n v] (get-in m path)]
       (when v
         (.setProperty props n v)))
     props)))
