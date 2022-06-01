(ns jackdaw.test.serde
  (:require
   [jackdaw.serdes.edn :as edn-serde]
   [jackdaw.serdes.json :as json-serde])
  (:import
   (org.apache.kafka.common.serialization Serdes
                                          ByteArraySerializer
                                          ByteArrayDeserializer)))


(set! *warn-on-reflection* false)

;; Access to various serdes

(defn resolver [topic-config]
  (let [serde-lookup {:edn (edn-serde/serde)
                      :json (json-serde/serde)
                      :long (Serdes/Long)
                      :string (Serdes/String)}]
    (merge
      topic-config
      {:key-serde (serde-lookup (:key-serde topic-config))
       :value-serde (serde-lookup (:value-serde topic-config))})))

;; Serialization/Deserialization
;;
;; Using a byte-array-serde allows us to use a single consumer to consume
;; from all topics. The test-machine knows how to further deserialize
;; the topic-info based on the topic-config supplied by the test author.

(def byte-array-serde
  "Byte-array key and value serde."
   {:key-serde (Serdes/ByteArray)
    :value-serde (Serdes/ByteArray)})

(def byte-array-serializer (ByteArraySerializer.))
(def byte-array-deserializer (ByteArrayDeserializer.))

(defn serialize-key
  "Serializes a key."
  [k {topic-name :topic-name
      key-serde :key-serde :as t}]
  (when k
    (-> (.serializer key-serde)
        (.serialize topic-name k))))

(defn serialize-value
  [v {topic-name :topic-name
      value-serde :value-serde :as t}]
  (when v
    (-> (.serializer value-serde)
        (.serialize topic-name v))))

(defn serializer
  "Serializes a message."
  [topic]
  (fn [record]
    (assoc record
           :key (serialize-key (:key record) topic)
           :value (serialize-value (:value record) topic))))

(defn deserialize-key
  "Deserializes a key."
  [k {topic-name :topic-name
      key-serde :key-serde}]
  (when k
    (-> (.deserializer key-serde)
        (.deserialize topic-name k))))

(defn deserialize-value
  "Deserializes a value."
  [v {topic-name :topic-name
      value-serde :value-serde}]
  (when v
    (-> (.deserializer value-serde)
        (.deserialize topic-name v))))

(defn deserializer
  "Deserializes a message."
  [topic]
  (fn [m]
    {:topic (:topic-name topic)
     :key (deserialize-key (:key m) topic)
     :value (deserialize-value (:value m) topic)
     :partition (:partition m 0)
     :offset (:offset m 0)
     :headers (:headers m {})}))

(defn deserializers
  "Returns a map of topics to the corresponding deserializer"
  [topic-config]
  (->> topic-config
       (map (fn [[k v]]
              [(:topic-name v)
               (deserializer v)]))
       (into {})))

(defn serializers
  "Returns a map of topic to the corresponding serializer"
  [topic-config]
  (->> topic-config
       (map (fn [[k v]]
              [(:topic-name v)
               (serializer v)]))
       (into {})))

(defn serde-map
  [topic-config]
  {:serializers (serializers topic-config)
   :deserializers (deserializers topic-config)})

(defn apply-serializers
  [serializers m]
  (let [topic (:topic m)
        serialize (get serializers (:topic-name topic))]
    (if (nil? serialize)
      (throw (IllegalArgumentException.
              (str "Message refers to unknown topic: " (:topic-name topic))))
      (serialize m))))

(defn apply-deserializers
  [deserializers m]
  (let [topic-name (:topic m)
        deserialize (get deserializers topic-name)]
    (if (nil? deserialize)
      (throw (IllegalArgumentException.
              (str "Record comes from unknown topic: " topic-name)))
      (deserialize m))))
