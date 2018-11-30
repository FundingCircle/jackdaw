(ns jackdaw.serdes-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [jackdaw.serdes :as serdes]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro.schema-registry :refer [mock-client]]
            [jackdaw.serdes.fn :as sfn])
  (:import [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))

(deftest lookup-serde

  (testing "Lookup a string serde using topic config"
    (let [serde (serdes/serde {:topic-name "stringy"
                               :jackdaw.serdes/type :jackdaw.serdes/string})]
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde (.getClass serde)))))

  (testing "Lookup a string serde using a keyword"
    (let [serde (serdes/serde :jackdaw.serdes/string)]
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde (.getClass serde)))))

  (testing "Looking up a serde fails when no serde is set"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Failed to find a legal mapping"
                          (serdes/serde {:topic-name "no-serde"}))))

  (testing "Looking up a serde fails when the serde set in topic config is not known"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unable to resolve serde for topic"
                          (serdes/serde {:topic-name "bad-serde"
                                         :jackdaw.serdes/type :unknown}))))

  (testing "Looking up a serde fails when the serde keyword is not known"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unable to resolve serde for topic"
                          (serdes/serde :unknown))))

  (testing "Looking up a custom serde from topic config"
    ;; its not a proper serde (an instance of org.apache.kafka.common.serialization.Serde)
    ;; but it tests the machanics and clojure lets you get away with it
    (let [serde (serdes/serde (merge serdes/+default-serdes+
                                     {:blackhole-sun (fn [conf]
                                                       (fn [v]
                                                         (str v (:blackhole-serde-config conf))))})
                              {:topic-name "stringy"
                               :blackhole-serde-config " got spaghettified!"
                               :jackdaw.serdes/type :blackhole-sun})]
      (is (fn? serde))
      (is (= "Matt got spaghettified!" (serde "Matt"))))))


(deftest resolve-serde

  (testing "Resolving a string serde for key and value"
    (let [serdefied (serdes/resolve {:topic-name "stringy"
                                     :key-serde :jackdaw.serdes/string
                                     :value-serde :jackdaw.serdes/string})]
      (is (map? serdefied))
      ;; things that shouldn't change don't change
      (is (= "stringy" (:topic-name serdefied)))
      ;; key and value serde keys are overwritten with the resolved impls
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde
             (.getClass (:key-serde serdefied))))
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde
             (.getClass (:value-serde serdefied))))))

  (testing "Resolving a string key and a ByteArray value serdes"
    (let [serdefied (serdes/resolve {:topic-name "stringy"
                                     :key-serde :jackdaw.serdes/string
                                     :value-serde :jackdaw.serdes/byte-array})]
      (is (map? serdefied))
      ;; things that shouldn't change don't change
      (is (= "stringy" (:topic-name serdefied)))
      ;; key and value serde keys are overwritten with the resolved impls
      (is (= org.apache.kafka.common.serialization.Serdes$StringSerde
             (.getClass (:key-serde serdefied))))
      (is (= org.apache.kafka.common.serialization.Serdes$ByteArraySerde
             (.getClass (:value-serde serdefied))))))

  (testing "When no serde is specified you get an error"
    (is (thrown-with-msg? IllegalArgumentException
                          #":key-serde is required in the topic config"
                          (serdes/resolve {})))
    (is (thrown-with-msg? IllegalArgumentException
                          #":value-serde is required in the topic config"
                          (serdes/resolve {:key-serde :jackdaw.serdes/string})))))


(deftest serialise-and-deserialise

  (testing "The resolved serdes actually work, e.g. to EDN and Back"
    (let [tc {:topic-name "endy"
              :key-serde :jackdaw.serdes/edn
              :value-serde :jackdaw.serdes/edn}
          resolved (serdes/resolve tc)
          value-serde (:value-serde resolved)
          value {:a 1 :b 2}
          ser (.serialize (.serializer value-serde) (:topic-name tc) value)
          des (.deserialize (.deserializer value-serde) (:topic-name tc) ser)]
      ;; data is serialised to a byte[] always
      (is (.isArray (.getClass ser)))
      (is (= value des))))

  (testing "A resoved custom serde works: To CAPS and back"
    (let [caps-serde (fn []
                       (Serdes/serdeFrom
                         (sfn/new-serializer
                           {:serialize (fn [_ _ data]
                                         (.getBytes (clojure.string/upper-case data)))})
                         (sfn/new-deserializer
                           {:deserialize (fn [_ _ data]
                                           (clojure.string/lower-case (String. data)))})))

          tc {:topic-name "IN_CAPS"
              :key-serde :caps
              :value-serde :caps}

          resolved (serdes/resolve {:caps (fn [_] (caps-serde))} tc)
          value-serde (:value-serde resolved)
          value "hello, world"
          ser (.serialize (.serializer value-serde) (:topic-name tc) value)
          des (.deserialize (.deserializer value-serde) (:topic-name tc) ser)]
        (is (= "HELLO, WORLD" (String. ser)))
        (is (= value des)))))


(deftest avro-serde

  (testing "That we can configure and resolve a working avro serde"
    (let [mock-schema-reg (mock-client)
          avro-serde (fn [key?]
                       (fn [topic-conf]
                         (avro/avro-serde
                           avro/+base-schema-type-registry+
                           (select-keys topic-conf
                                        [:avro.schema-registry/client
                                         :avro.schema-registry/url])
                           {:key? key?
                            :avro/schema ((if key?
                                            :avro/key-schema
                                            :avro/value-schema) topic-conf)})))
          tc {:topic-name "avro-data"
              :key-serde :avro-key
              :value-serde :avro-value
              ;; Avro requires a few more settings in the topic data
              :avro.schema-registry/client mock-schema-reg
              :avro.schema-registry/url "localhost:8081"
              ;; pretty trivial schemas
              :avro/key-schema (json/write-str {:type "long"})
              :avro/value-schema (json/write-str {:type "string"})}
          resolved (serdes/resolve
                     {:avro-key (avro-serde true)
                      :avro-value (avro-serde false)}
                     tc)

          key-serde (:key-serde resolved)
          k (long 123456789)
          ser-k (.serialize (.serializer key-serde) (:topic-name tc) k)
          des-k (.deserialize (.deserializer key-serde) (:topic-name tc) ser-k)

          value-serde (:value-serde resolved)
          value "hello, world"
          ser-val (.serialize (.serializer value-serde) (:topic-name tc) value)
          des-val (.deserialize (.deserializer value-serde) (:topic-name tc) ser-val)]

      ;; As well as doing a round trip for the data, the schemas should get registered
      (let [registered-schema (.getLatestSchemaMetadata mock-schema-reg "avro-data-key")]
        (is (not (nil? registered-schema)))
        (is (= "\"long\"" (.getSchema registered-schema))))
      (is (.isArray (.getClass ser-k)))
      (is (= k des-k))

      (let [registered-schema (.getLatestSchemaMetadata mock-schema-reg "avro-data-value")]
        (is (not (nil? registered-schema)))
        (is (= "\"string\"" (.getSchema registered-schema))))
      (is (.isArray (.getClass ser-val)))
      (is (= value des-val)))))
