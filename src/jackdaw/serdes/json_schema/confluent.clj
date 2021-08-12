(ns jackdaw.serdes.json-schema.confluent
  "Implements a Confluent JSON SCHEMA REGISTRY SerDes (Serializer/Deserializer)."
  (:require [clojure.walk :as walk]
            [jackdaw.serdes.fn :as jsfn]
            [jsonista.core :as jsonista])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.common.serialization
            Serializer Deserializer Serdes]
           [io.confluent.kafka.serializers.json
            KafkaJsonSchemaDeserializer KafkaJsonSchemaSerializer]
           [com.fasterxml.jackson.databind ObjectMapper]))

(set! *warn-on-reflection* true)

(defn- base-config [registry-url]
  {"schema.registry.url" registry-url
   "json.fail.invalid.schema" true})

(def parse-schema-str
  (memoize
   (fn [schema-str]
     (when schema-str
       (jsonista/read-value schema-str)))))

(def json-envelope-mapper
  (jsonista/object-mapper {:encode-key-fn name}))

(defn ->json-schema-jackson-envelope [schema data]
  (.valueToTree
   ^ObjectMapper json-envelope-mapper
   {:schema schema
    :payload data}))

(defn serializer
  "Returns a JSON schema-registry serializer."
  [serde-config]
  (let [{:keys [registry-url read-only? key?
                json-schema serializer-properties
                registry-client]} serde-config
        serializer-config (-> (base-config registry-url)
                              (merge serializer-properties))
        base-serializer (KafkaJsonSchemaSerializer. registry-client)
        methods {:close     (fn [_]
                              (.close base-serializer))
                 :configure (fn [_ config key?]
                              (.configure base-serializer config key?))
                 :serialize (fn [_ topic data]
                              (when read-only?
                                (throw (ex-info "Cannot serialize from a read-only serde"
                                                {:serde-config serde-config})))
                              (try
                                (let [envelope (->json-schema-jackson-envelope json-schema data)]
                                  (.serialize base-serializer topic envelope))
                                (catch Throwable e
                                  (let [data {:topic topic :clj-data data}]
                                    (throw (ex-info (.getMessage e) data e))))))}
        clj-serializer (jsfn/new-serializer methods)]
    (.configure ^Serializer clj-serializer serializer-config key?)
    clj-serializer))

(defn deserializer
  "Returns a JSON schema-registry deserializer."
  [serde-config]
  (let [{:keys [registry-url key? deserializer-properties
                registry-client json-schema]} serde-config
        deserializer-config (-> (base-config registry-url)
                                (merge
                                 (cond-> deserializer-properties
                                   ;; Detect top level array types
                                   (= "array" (get json-schema "type"))
                                   (assoc "json.value.type" "com.fasterxml.jackson.databind.node.ArrayNode"))))
        base-deserializer (KafkaJsonSchemaDeserializer. registry-client)
        methods {:close       (fn [_]
                                (.close base-deserializer))
                 :configure   (fn [_ config key?]
                                (.configure base-deserializer config key?))
                 :deserialize (fn [_ topic raw-data]
                                (try
                                  (let [json-data
                                        (.deserialize base-deserializer
                                                      topic
                                                      #^bytes raw-data)]

                                    ;; Clojurify deserialized data
                                    (cond
                                      (instance? java.util.LinkedHashMap json-data)
                                      (->> json-data
                                           (into {})
                                           walk/keywordize-keys)
                                      ;; There might be a more efficient way for this converison
                                      (instance? com.fasterxml.jackson.databind.node.ArrayNode json-data)
                                      (->> json-data
                                           str
                                           jsonista/read-value
                                           walk/keywordize-keys)
                                      :else
                                      json-data))
                                  (catch Throwable e
                                    (let [data {:topic topic :raw-data raw-data}]
                                      (throw (ex-info (.getMessage e) data e))))))}
        clj-deserializer (jsfn/new-deserializer methods)]
    (.configure ^Deserializer clj-deserializer deserializer-config key?)
    clj-deserializer))

(defn serde
  "Returns a Confluent JSON Schema serde."
  [schema-registry-url schema key? & [{:keys [read-only?
                                              schema-registry-client
                                              serializer-properties
                                              deserializer-properties]}]]
  (when-not schema-registry-url
    (throw
     (IllegalArgumentException.
      ":schema-registry/url is required in the registry config")))

  (let [config {:key? key?
                :registry-url schema-registry-url
                :registry-client schema-registry-client
                :read-only? read-only?
                :json-schema (parse-schema-str schema)}

        json-serializer (serializer (assoc config :serializer-properties serializer-properties))
        json-deserializer (deserializer (assoc config :deserializer-properties deserializer-properties))]
    (Serdes/serdeFrom json-serializer json-deserializer)))
