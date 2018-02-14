(ns jackdaw.serdes.fn
  (:import
   [org.apache.kafka.common.serialization Deserializer
    Serdes
    Serializer]))

;; we're going to AOT this ns, so it's very important that it doesn't
;; :require anything, because AOT'ing other peoples code is a Bad
;; Idea.

(deftype FnSerializer [f]
  Serializer
  (close [this])
  (configure [this configs key?])
  (serialize [this _topic data]
    (when data
      (f data))))

(deftype FnDeserializer [f]
  Deserializer
  (close [this])
  (configure [this configs key?])
  (deserialize [this _topic data]
    (when data
      (f data))))


