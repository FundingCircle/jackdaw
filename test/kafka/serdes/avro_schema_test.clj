(ns kafka.serdes.avro-schema-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [environ.core :as env]
            [kafka.serdes.avro :as avro]
            [kafka.serdes.avro-schema :refer :all]
            [kafka.serdes.registry :as registry]
            [clj-uuid :as uuid])
  (:import
    (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient MockSchemaRegistryClient)
    (org.apache.avro Schema$Parser)
    (org.apache.avro.generic GenericData$Record GenericData$Array GenericData$EnumSymbol)
    (org.apache.avro.util Utf8)))

(def schema (slurp (io/resource "resources/example_schema.avsc")))

(def record (edn/read-string (slurp (io/resource "resources/example_record.edn"))))

(defn with-mock-client [config]
  (assoc config
         :schema.registry/client (MockSchemaRegistryClient.)))

(defn with-real-client [config]
  (assoc config
         :schema.registry/client
         (registry/client config 5)))

(def avro-config
  {:avro/schema schema
   :avro/is-key false
   :schema.registry/url "http://localhost:8081"})

(deftest map-roundtrip-test
  (testing "Map is the same after conversion to generic record and back"
    (is (= record
           (-> (map->generic-record schema record)
               (generic-record->map)))))

  (testing "schema can be serialized by registry client"
    (let [serde (avro/avro-serde (with-mock-client avro-config) false)]
      (let [msg {:customer-id (uuid/v4)
                 :address {:value "foo"
                           :key-path "foo.bar.baz"}}]

        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))

(deftest ^:integration schema-registry
  (testing "schema registry set in environment"
    (with-redefs [env/env {:schema-registry-url "http://localhost:8081"}]
      (let [serde (avro/avro-serde (with-real-client avro-config) false)]
        (let [msg {:customer-id (uuid/v4)
                   :address {:value "foo"
                             :key-path "foo.bar.baz"}}]
          (let [serialized (-> (.serializer serde)
                               (.serialize "foo" msg))
                deserialized (-> (.deserializer serde)
                                 (.deserialize "foo" serialized))]
            (is (= deserialized msg)))))))

  (testing "schema registry set in config"
    (with-redefs [env/env {:schema-registry-url "http://registry.example.com:8081"}]
      (let [serde (avro/avro-serde (with-real-client avro-config) false)]
        (let [msg {:customer-id (uuid/v4)
                   :address {:value "foo"
                             :key-path "foo.bar.baz"}}]
          (let [serialized (-> (.serializer serde)
                               (.serialize "foo" msg))
                deserialized (-> (.deserializer serde)
                                 (.deserialize "foo" serialized))]
            (is (= deserialized msg))))))))

(deftest marshall-string-test
  (testing "marshalling a string"

    (let [parser (Schema$Parser.)
          avro-edn {:type "string", :name "postcode" :namespace "com.fundingcircle"}
          avro-schema (.parse parser ^String (json/write-str avro-edn))]
      (is (= "test-string" (marshall avro-schema "test-string")))))

  (testing "marshalling a string with UUID logicalType"

    (let [parser (Schema$Parser.)
          avro-edn {:type "string" :name "id" :namespace "com.fundingcircle"
                    :logicalType "kafka.serdes.avro.UUID"}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          payload (uuid/v4)]
      (is (= (str payload) (marshall avro-schema payload)))))

  (testing "marshalling a record containing a string with UUID logicalType"

    (let [parser (Schema$Parser.)
          avro-edn {:type "record" :name "recordStringLogicalTypeTest" :namespace "com.fundingcircle"
                    :fields [{:name "id" :namespace "com.fundingcircle"
                              :type {:type "string" :logicalType "kafka.serdes.avro.UUID"}}]}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          id (uuid/v4)
          expected (GenericData$Record. avro-schema)]
      (.put expected "id" (str id))
      (is (= expected (marshall avro-schema {:id id}))))))

(deftest marshall-long-with-integer-test
  ; This can be caused by Ruby applications that send a JSON payload over
  ; RabbitMQ
  (testing "marshalling a long"

    (let [parser (Schema$Parser.)
          avro-edn {:type "long", :name "amount_cents" :namespace "com.fundingcircle"}
          avro-schema (.parse parser ^String (json/write-str avro-edn))]
      (is (= 4 (marshall avro-schema (java.lang.Integer. 4)))))))

(deftest marshall-enum-test
  (testing "marshalling an enum"

    ; Accept dasherised or underscored input keywords and strings
    (let [parser (Schema$Parser.)
          avro-edn {:namespace "com.fundingcircle" :name "industry_code_version"
                    :type "enum" :symbols ["SIC_2003"]}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          expected (GenericData$EnumSymbol. avro-schema "SIC_2003")]
      (is (= expected (marshall avro-schema "SIC-2003")))
      (is (= expected (marshall avro-schema "SIC_2003")))
      (is (= expected (marshall avro-schema :SIC-2003)))
      (is (= expected (marshall avro-schema :SIC_2003))))))

(deftest marshall-array-test
  (testing "marshalling an array"

    (let [parser (Schema$Parser.)
          avro-edn {:namespace "com.fundingcircle" :name "credit_score_guarantors"
                    :type "array" :items "string"}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          expected (GenericData$Array. avro-schema ["0.4" "56.7"])]
      (is (= expected (marshall avro-schema ["0.4" "56.7"]))))))

(deftest marshall-map-test
  (testing "marshalling a map returns the map"
    (let [avro-edn {:type "map" :values "long"}
          avro-schema (.parse (Schema$Parser.) (json/write-str avro-edn))
          expected {"foo" 1 "bar" 2}]
      (is (= expected (marshall avro-schema expected))))))

(deftest marshall-record-test
  (testing "marshalling record with unknown field triggers error"
    (let [parser (Schema$Parser.)
          avro-edn {:type "record"
                    :name "Foo"
                    :fields [{:name "bar" :type "string"}]}
          avro-schema (.parse parser (json/write-str avro-edn))]
      (is (thrown-with-msg? AssertionError
                            #"Field garbage not known in Foo"
                            (marshall avro-schema {:garbage "yolo"}))))))

(deftest marshall-union-test
  (let [parser (Schema$Parser.)
        avro-edn ["null" "long"]
        avro-schema (.parse parser ^String (json/write-str avro-edn))]

    (testing "marshalling a union"
      (is (= 1 (marshall avro-schema 1)))
      (is (= nil (marshall avro-schema nil))))

    (testing "marshalling unrecognized union type throws exception"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"No matching union schema"
                            (marshall avro-schema "foo"))))))

(deftest unmarshall-string-test
  (testing "unmarshalling a string"

    (let [parser (Schema$Parser.)
          avro-edn {:type "record", :name "stringtest" :namespace "com.fundingcircle"
                    :fields [{:name "id" :type "string"}]}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          generic-record (GenericData$Record. avro-schema)]
      (.put generic-record "id" "test-string")
      (is (= {:id "test-string"} (generic-record->map generic-record)))))

  (testing "unmarshalling a string with UUID logicalType"

    (let [parser (Schema$Parser.)
          avro-edn {:type "record", :name "stringtest" :namespace "com.fundingcircle"
                    :fields [{:name "id" :type "string" :logicalType "kafka.serdes.avro.UUID"}]}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          generic-record (GenericData$Record. avro-schema)
          id (uuid/v4)]
      (.put generic-record "id" (str id))
      (is (= {:id id} (generic-record->map generic-record))))))


(deftest unmarshall-enum-test
  ; Always output dasherised keyword enums
  (testing "unmarshalling an enum"

    (let [parser (Schema$Parser.)
          avro-edn {:type "enum" :name "industry_code_version" :symbols ["SIC_2003"]}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          generic-record (GenericData$EnumSymbol. avro-schema "SIC_2003")]
      (is (= :SIC-2003 (value-unmarshal generic-record))))

    (let [parser (Schema$Parser.)
          avro-edn {:type "record", :name "enumtest" :namespace "com.fundingcircle"
                    :fields [{:name "industry_code_version"
                              :type {:type "enum" :name "industry_code_version" :symbols ["SIC_2003"]}}]}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          generic-record (marshall avro-schema {:industry-code-version :SIC-2003})]
      (is (= {:industry-code-version :SIC-2003} (generic-record->map generic-record))))))

(deftest unmarshall-map-test
  (testing "unmarshalling a map"
    (let [m {"foo" 1 "bar" 2}]
      (is (= m (value-unmarshal m))))))

(deftest unmarshall-array-test
  (testing "unmarshalling an array"

    (let [parser (Schema$Parser.)
          avro-edn {:namespace "com.fundingcircle" :name "credit_score_guarantors"
                    :type "array" :items "string"}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          generic-record (GenericData$Array. avro-schema ["0.4" "56.7"])]
      (is (= ["0.4" "56.7"] (value-unmarshal generic-record))))))

(deftest unmarshall-utf8-test
  (testing "unmarshalling a utf8 character set"

    (let [parser (Schema$Parser.)
          avro-edn {:namespace "com.fundingcircle" :name "euro" :type "string"}
          avro-schema (.parse parser ^String (json/write-str avro-edn))
          b (byte-array [0xE2 0x82 0xAC])
          utf8 (Utf8. b)]
      (is (= (String. b) (value-unmarshal utf8))))))
