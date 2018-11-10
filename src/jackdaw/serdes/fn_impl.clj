(ns jackdaw.serdes.fn-impl
  (:import
   [org.apache.kafka.common.serialization Deserializer
    Serdes
    Serializer]))

;; Kafka requires serdes to load from their own classloader, which
;; requires AOT. We don't want to transitively AOT other libs such as
;; tools.reader.edn, or nippy, etc, so provide a dumb box that we can
;; AOT and then inject using clojure fns.

(defrecord FnSerializer [close configure serialize]
  Serializer
  (close [{close :close :as this}]
    (when close
      (close this)))
  (configure [{configure :configure :as this} configs key?]
    (when configure
      (configure this configs key?)))
  (serialize [{serialize :serialize :as this} topic data]
    (assert serialize)
    (when data
      (serialize this topic data))))

(defrecord FnDeserializer [close configure deserialize]
  Deserializer
  (close [{close :close :as this}]
    (when close
      (close this)))
  (configure [{configure :configure :as this} configs key?]
    (when configure
      (configure this configs key?)))
  (deserialize [{deserialize :deserialize :as this} topic data]
    (when data
      (deserialize this topic data))))
