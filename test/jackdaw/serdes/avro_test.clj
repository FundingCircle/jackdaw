(ns jackdaw.serdes.avro-test
  (:require [clojure.test :refer [deftest is testing] :as test]
            [clj-uuid :as uuid]
            [clojure.data :refer [diff]]
            [clojure.data.json :as json]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro.schema-registry :as reg])
  (:import [java.nio ByteBuffer]
           [java.util Collection]
           [org.apache.avro Schema$Parser Schema]
           [org.apache.avro.generic
            GenericData$EnumSymbol GenericData$Record GenericData$Array]
           [org.apache.avro.util Utf8]))

(set! *warn-on-reflection* false)

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(defn ->generic-record [avro-schema m]
  (let [record (GenericData$Record. avro-schema)]
    (doseq [[k v] m]
      (.put record k v))
    record))

(def +registry+
  (merge avro/+base-schema-type-registry+
         avro/+UUID-type-registry+))

(def schema-type
  (avro/make-coercion-stack
   +registry+))

(defn ->serde
  ([schema-str]
   (->serde schema-str (reg/mock-client)))

  ([schema-str registry-client]
   (let [serde-config {:avro/schema schema-str
                       :key?        false}]
     (->serde schema-str registry-client serde-config)))

  ([schema-str registry-client serde-config]
   (let [serde-config (merge {:avro/schema schema-str
                              :key?        false}
                             serde-config)
         schema-registry-config
         {:avro.schema-registry/client registry-client
          :avro.schema-registry/url    "localhost:8081"}]
     (avro/serde +registry+ schema-registry-config serde-config))))

(defn deserialize [serde topic x]
  (let [deserializer (.deserializer serde)]
    (.deserialize deserializer
                  topic
                  x)))

(defn serialize [serde topic x]
  (let [serializer (.serializer serde)]
    (.serialize serializer
                topic
                x)))

(defn round-trip [serde topic x]
  (let [serializer (.serializer serde)
        deserializer (.deserializer serde)]
    (.deserialize deserializer topic
                  (.serialize serializer topic x))))

(defn decoupled-round-trip [write-serde
                            read-serde
                            topic x xform]
  (let [serializer (.serializer write-serde)
        deserializer (.deserializer read-serde)]
    (->> (.serialize serializer topic x)
         (.deserialize deserializer topic)
         xform)))

(defn byte-buffer->string [^ByteBuffer buffer]
  (String. (.array buffer)))

(defmethod test/assert-expr 'thrown-with-msg-and-data? [msg form]
  (let [klass (nth form 1)
        re (nth form 2)
        data (nth form 3)
        body (nthnext form 4)]
    `(try ~@body
          (test/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch ~klass e#
            (let [m# (.getMessage e#)
                  data# (ex-data e#)]
              (if (re-find ~re m#)
                (if (= ~data data#)
                  (test/do-report {:type :pass, :message ~msg,
                                   :expected '~form, :actual e#})
                  (test/do-report {:type :fail, :message ~msg,
                                   :expected '~data, :actual data#
                                   :diff (diff ~data data#)}))
                (test/do-report {:type :fail, :message ~msg,
                                 :expected '~form, :actual e#})))
            e#))))

(deftest schema-type-test
  (testing "schemaless"
    (is (= (avro/clj->avro (schema-type nil) "hello" [])
           "hello"))
    (is (= 1 (avro/avro->clj (schema-type nil) 1))))
  (testing "boolean"
    (let [avro-schema (parse-schema {:type "boolean"})
          schema-type (schema-type avro-schema)
          clj-data true
          avro-data true]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))
  (testing "double"
    (let [avro-schema (parse-schema {:type "double"})
          schema-type (schema-type avro-schema)
          clj-data 2.0
          avro-data 2.0]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))
  (testing "float"
    (let [avro-schema (parse-schema {:type "float"})
          schema-type (schema-type avro-schema)
          clj-data (float 2)
          avro-data (float 2)]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))
  (testing "int"
    (let [avro-schema (parse-schema {:type "int"})
          schema-type (schema-type avro-schema)
          clj-data (int 2)
          avro-data 2]
      (is (avro/match-clj? schema-type clj-data))
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))

  (testing "coercable int"
    (let [avro-schema (parse-schema {:type "int"})
          schema-type (schema-type avro-schema)
          clj-data (bigint "2")
          avro-data 2]
      (is (avro/match-clj? schema-type clj-data))
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))

  (testing "coercable long"
    (let [avro-schema (parse-schema {:type "long"})
          schema-type (schema-type avro-schema)
          clj-data (bigint (str (inc Integer/MAX_VALUE)))
          avro-data (long (inc Integer/MAX_VALUE))]
      (avro/match-clj? schema-type clj-data)
      (is (avro/match-clj? schema-type clj-data))
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))

  (testing "long"
    (let [avro-schema (parse-schema {:type "long"
                                     :name "amount_cents"
                                     :namespace "com.fundingcircle"})
          schema-type (schema-type avro-schema)
          clj-data 4
          avro-data (Integer. 4)]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))

      (is (int? (avro/clj->avro schema-type (byte clj-data) [])))
      (is (int? (avro/clj->avro schema-type (short clj-data) [])))
      (is (int? (avro/clj->avro schema-type (int clj-data) [])))))

  (testing "string"
    (let [avro-schema (parse-schema {:type "string"
                                     :name "postcode"
                                     :namespace "com.fundingcircle"})
          schema-type (schema-type avro-schema)
          clj-data "test-string"
          avro-data "test-string"]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))
  (testing "unmarshalling a utf8 character set"
    (let [avro-schema (parse-schema {:namespace "com.fundingcircle"
                                     :name "euro"
                                     :type "string"})
          schema-type (schema-type avro-schema)
          b (byte-array [0xE2 0x82 0xAC])
          utf8 (Utf8. b)]
      (is (= (String. b) (avro/avro->clj schema-type utf8)))))
  (testing "null"
    (let [avro-schema (parse-schema {:type "null"})
          schema-type (schema-type avro-schema)
          clj-data nil
          avro-data nil]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))
  (testing "array"
    (let [avro-schema (parse-schema {:namespace "com.fundingcircle"
                                     :name "credit_score_guarantors"
                                     :type "array"
                                     :items "string"})
          schema-type (schema-type avro-schema)
          clj-data ["0.4" "56.7"]
          avro-data (GenericData$Array. ^Schema avro-schema
                                        ^Collection clj-data)]
      (is (avro/match-clj? schema-type clj-data))
      (is (avro/match-clj? schema-type (seq clj-data)))
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))
  (testing "nested array"
    (let [nested-schema-json {:name "nestedRecord"
                              :type "record"
                              :fields [{:name "a"
                                        :type "long"}]}
          nested-schema-parsed (parse-schema nested-schema-json)

          array-schema-json {:name "credit_score_guarantors"
                             :type "array"
                             :items nested-schema-json}
          array-schema-parsed (parse-schema array-schema-json)

          avro-schema (parse-schema {:name "testRecord"
                                     :type "record"
                                     :fields [{:name "stringField"
                                               :type "string"}
                                              {:name "longField"
                                               :type "long"}
                                              {:name "recordField"
                                               :type array-schema-json}]})
          schema-type (schema-type avro-schema)


          clj-data {:stringField "foo"
                    :longField 123
                    :recordField [{:a 1}]}
          avro-data (->generic-record avro-schema {"stringField" "foo"
                                                   "longField" 123
                                                   "recordField" (GenericData$Array. ^Schema array-schema-parsed
                                                                                     ^Collection [(->generic-record nested-schema-parsed {"a" 1})])})]

      (is (avro/match-clj? schema-type clj-data))
      (is (avro/match-clj? schema-type {:stringField "foo"
                                        :longField 123
                                        :recordField [{:b 1}]}))

      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))))
  (testing "map"
    (let [nested-schema-json {:name "nestedRecord"
                              :type "record"
                              :fields [{:name "a"
                                        :type "long"}]}
          nested-schema-parsed (parse-schema nested-schema-json)

          avro-schema (parse-schema {:type "map" :values nested-schema-json})
          schema-type (schema-type avro-schema)
          clj-data {"foo" {:a 1} "bar" {:a 2}}
          avro-data {(Utf8. "foo") (->generic-record nested-schema-parsed {"a" 1}) (Utf8. "bar") (->generic-record nested-schema-parsed {"a" 2})}
          avro-data-str-keys (reduce-kv (fn [acc k v]
                                          (assoc acc (str k) v))
                                        {}
                                        avro-data)]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data-str-keys (avro/clj->avro schema-type clj-data [])))))
  (testing "union"
    (let [record-1-schema {:name "recordOne"
                           :type "record"
                           :namespace "com.fundingcircle"
                           :fields [{:name "a"
                                     :type {:name "enumOne"
                                            :type "enum"
                                            :symbols ["x"]}
                                     :symbols ["x"]}]}
          record-2-schema {:name "recordTwo"
                           :type "record"
                           :namespace "com.fundingcircle"
                           :fields [{:name "a"
                                     :type {:name "enumTwo"
                                            :type "enum"
                                            :symbols ["y"]}}
                                    {:name "b"
                                     :type ["string" "null"]}]}
          record-3-schema {:name "recordThree"
                           :type "record"
                           :namespace "com.fundingcircle"
                           :fields [{:name "a"
                                     :type {:name "enumThree"
                                            :type "enum"
                                            :symbols ["y"]}}
                                    {:name "c"
                                     :type ["string" "null"]}]}
          enum-schema {:name "enum"
                       :type "enum"
                       :symbols ["a" "b" "c"]}
          avro-schema (parse-schema ["long"
                                     "string"
                                     enum-schema
                                     record-1-schema
                                     record-2-schema
                                     record-3-schema])
          schema-type (schema-type avro-schema)
          clj-data-long 123
          avro-data-long 123
          clj-data-string "hello"
          avro-data-string (Utf8. "hello")
          clj-data-num-as-string "123"
          avro-data-num-as-string (Utf8. "123")
          clj-data-enum :a
          avro-data-enum (GenericData$EnumSymbol. (parse-schema enum-schema) "a")]
      (is (= clj-data-long (avro/avro->clj schema-type avro-data-long)))
      (is (= avro-data-long (avro/clj->avro schema-type clj-data-long [])))
      (is (= clj-data-string (avro/avro->clj schema-type avro-data-string)))
      (is (= (str avro-data-string) (avro/clj->avro schema-type clj-data-string [])))
      (is (= clj-data-num-as-string (avro/avro->clj schema-type avro-data-num-as-string)))
      (is (= (str avro-data-num-as-string) (avro/clj->avro schema-type clj-data-num-as-string [])))
      (is (= avro-data-enum (avro/clj->avro schema-type clj-data-enum [])))
      (is (= clj-data-enum (avro/avro->clj schema-type avro-data-enum)))
      (is (= (->generic-record (parse-schema record-1-schema) {"a" "x"})
             (avro/clj->avro schema-type {:a :x} [])))
      (is (= {:a :x}
             (avro/avro->clj schema-type (->generic-record (parse-schema record-1-schema) {"a" "x"}))))
      (is (= (->generic-record (parse-schema record-2-schema) {"a" "y" "b" "test"})
             (avro/clj->avro schema-type {:a :y :b "test"} [])))
      (is (= {:a :y :b "test"}
             (avro/avro->clj schema-type (->generic-record (parse-schema record-2-schema) {"a" "y" "b" "test"}))))
      (is (= (->generic-record (parse-schema record-3-schema) {"a" "y" "c" "test"})
             (avro/clj->avro schema-type {:a :y :c "test"} [])))
      (is (= {:a :y :c "test"}
             (avro/avro->clj schema-type (->generic-record (parse-schema record-3-schema) {"a" "y" "c" "test"}))))
      (is (thrown? Exception (avro/clj->avro schema-type {:a :y} [])))
      (is (thrown? Exception (avro/clj->avro schema-type {:a :x :d "test"} [])))
      (is (thrown? Exception (avro/clj->avro schema-type {:a :x :b "test"} [])))))
  (testing "marshalling unrecognized union type throws exception"
    (let [avro-schema (parse-schema ["null" "long"])
          schema-type (schema-type avro-schema)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"java.lang.String is not a valid type for union \[NULL, LONG\]"
                            (avro/clj->avro schema-type "foo" [])))))
  (testing "enum"
    (let [enum-schema {:type "enum"
                       :name "industry_code_version"
                       :symbols ["SIC_2003"]}
          avro-schema (parse-schema {:type "record"
                                     :name "enumtest"
                                     :namespace "com.fundingcircle"
                                     :fields [{:name "industry_code_version"
                                               :type enum-schema}]})
          schema-type (schema-type avro-schema)
          clj-data {:industry-code-version :SIC-2003}
          avro-enum (GenericData$EnumSymbol. avro-schema "SIC_2003")
          avro-data (->generic-record avro-schema {"industry_code_version" avro-enum})]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))
      (is (= avro-data (avro/clj->avro schema-type {:industry-code-version "SIC-2003"} [])))
      (is (thrown? Exception (avro/clj->avro schema-type {:industry-code-version "invalid"} [])))))
  (testing "record"
    (let [nested-schema-json {:name "nestedRecord"
                              :type "record"
                              :fields [{:name "a"
                                        :type "long"}]}
          nested-schema-parsed (parse-schema nested-schema-json)
          avro-schema (parse-schema {:name "testRecord"
                                     :type "record"
                                     :fields [{:name "stringField"
                                               :type "string"}
                                              {:name "longField"
                                               :type "long"}
                                              {:name "optionalField"
                                               :type ["null" "int"]
                                               :default nil}
                                              {:name "defaultField"
                                               :type "long"
                                               :default 1}
                                              {:name "recordField"
                                               :type nested-schema-json}]})
          schema-type (schema-type avro-schema)
          clj-data {:stringField "foo"
                    :longField 123
                    :recordField {:a 1}}
          clj-data-opt (assoc clj-data :optionalField (long (Integer/MAX_VALUE)))
          avro-data (->generic-record avro-schema {"stringField" "foo"
                                                   "longField" 123
                                                   "defaultField" 1
                                                   "recordField" (->generic-record nested-schema-parsed {"a" 1})})]
      (is (avro/match-clj? schema-type clj-data))
      (is (avro/match-clj? schema-type clj-data-opt))
      (is (not (avro/match-clj? schema-type (assoc clj-data-opt :optionalField (inc (long Integer/MAX_VALUE))))))
      (is (not (avro/match-clj? schema-type (assoc clj-data-opt :optionalField (dec (long Integer/MIN_VALUE))))))
      (is (= (assoc clj-data :optionalField nil :defaultField 1) (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))
      (is (instance? Integer (.get (avro/clj->avro schema-type clj-data-opt []) "optionalField")))))
  (testing "marshalling record with unknown field triggers error"
    (let [avro-schema (parse-schema {:type "record"
                                     :name "Foo"
                                     :fields [{:name "bar" :type "string"}]})
          schema-type (schema-type avro-schema)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Field garbage not known in Foo"
                            (avro/clj->avro schema-type {:garbage "yolo"} [])))))
  (testing "uuid"
    (let [avro-schema (parse-schema {:type "string",
                                     :logicalType "uuid"})
          schema-type (schema-type avro-schema)
          clj-data uuid/+null+
          avro-data (uuid/to-string uuid/+null+)]
      (is (avro/match-clj? schema-type clj-data))
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data []))))))

(def bananas-schema
  {:type "record"
   :name "banana"
   :fields [{:name "color"
             :type "string"}]})

(def complex-schema
  {:name "testRecord"
   :type "record"
   :fields [{:name "string_field"
             :type "string"}
            {:name "long_field"
             :type "long"}
            {:name "optional_field"
             :type ["null" "int"]
             :default nil}
            {:name "nil_field"
             :type "null"}
            {:name "default_field"
             :type "long"
             :default 1}
            {:name "bytes_field"
             :type "bytes"}
            {:name "enum_field"
             :type {:type "enum"
                    :name "weird_values"
                    :symbols ["a_1" "B3"]}}
            {:name "map_field"
             :type ["null" {:type "map"
                            :values bananas-schema}]}
            {:name "array_field"
             :type ["null" {:name "subrecords"
                            :type "array"
                            :items "banana"}]}
            {:name "uuid_field"
             :type {:type "string",
                    :logicalType "uuid"}}]})

(def complex-schema-str (json/write-str complex-schema))

(deftest correct-union-record-is-picked-for-coercion
  (let [schema {:type   "record",
                :name   "myrecord",
                :fields [{:name "myunion",
                          :type [{:type   "record",
                                  :name   "recordtype1",
                                  :fields [{:name "field1", :type "string"}]}
                                 {:type   "record",
                                  :name   "recordtype2",
                                  :fields [{:name "field2", :type "string"}]}]}]}
        serde  (->serde (json/write-str schema))]

    (is (= {:myunion {:field1 "hello"}}
           (round-trip serde "whatever" {:myunion {:field1 "hello"}})))

    (is (= {:myunion {:field2 "hello"}}
           (round-trip serde "whatever" {:myunion {:field2 "hello"}})))))

(deftest record-serde-test
  (let [serde (->serde complex-schema-str)
        valid-map {:string-field "hello"
                   :long-field 3
                   :default-field 1
                   :nil-field nil
                   :bytes-field (ByteBuffer/wrap (.getBytes "hello"))
                   :map-field {"banana" {:color "yellow"}
                               "ripe b4nana$" {:color "yellow-green"}}
                   :enum-field :a-1
                   :optional-field 3
                   :array-field [{:color "yellow"}]
                   :uuid-field uuid/+null+}
        test-round-trip (fn [re {:keys [topic clj-data] :as data}]
                          (is (thrown-with-msg-and-data? clojure.lang.ExceptionInfo
                                                         re
                                                         data
                                                         (round-trip (->serde complex-schema-str)
                                                                     topic
                                                                     clj-data))))]
    (is (= (update (round-trip serde
                               "bananas"
                               valid-map)
                   :bytes-field byte-buffer->string)

           {:string-field "hello"
            :long-field 3
            :default-field 1
            :nil-field nil
            :bytes-field "hello"
            :map-field {"banana" {:color "yellow"}
                        "ripe b4nana$" {:color "yellow-green"}}
            :enum-field :a-1
            :optional-field 3
            :array-field [{:color "yellow"}]
            :uuid-field uuid/+null+}))

    (test-round-trip #"java.lang.Long is not a valid type for string"
                     {:path [:map-field "banana" :color]
                      :topic "bananas"
                      :data 3
                      :clj-data (assoc valid-map :map-field {"banana" {:color 3}})})

    (test-round-trip #"Field tasty not known in banana"
                     {:path [:map-field "banana"]
                      :topic "bananas"
                      :clj-data (assoc valid-map :map-field {"banana" {:color "yellow" :tasty true}})})

    (test-round-trip #"Field color type:STRING pos:0 not set and has no default value"
                     {:path [:map-field "bad banana"]
                      :topic "bananas"
                      :clj-data (assoc valid-map :map-field {"bad banana" {}
                                                             "good banana" {:color "yellow"}})})

    (test-round-trip #"clojure.lang.Keyword \(:invalid-key\) is not a valid map key type, only string keys are supported"
                     {:path [:map-field]
                      :topic "bananas"
                      :clj-data (assoc valid-map :map-field {:invalid-key {:color "yellow"}})})

    (test-round-trip #"java\.lang\.Long is not a valid type for string"
                     {:path [:array-field 0 :color]
                      :data 3
                      :topic "bananas"
                      :clj-data (assoc valid-map :array-field [{:color 3}])})

    (test-round-trip #"java\.lang\.Long is not a valid type for record"
                     {:path [:array-field 1]
                      :topic "bananas"
                      :clj-data (assoc valid-map :array-field [{:color "yellow"} 3])})

    (test-round-trip #"nil is not a valid type for string"
                     {:path [:array-field 0 :color]
                      :data nil
                      :topic "bananas"
                      :clj-data (assoc valid-map :array-field [{:color nil}])})

    (test-round-trip #"java.lang.Long is not a valid type for nil"
                     {:path [:nil-field]
                      :data 3
                      :topic "bananas"
                      :clj-data (assoc valid-map :nil-field 3)})

    (test-round-trip #"java\.lang\.String is not a valid type for union \[NULL, ARRAY\]"
                     {:path [:array-field]
                      :topic "bananas"
                      :clj-data (assoc valid-map :array-field "string")})
    (test-round-trip #"java\.lang\.String is not a valid type for uuid"
                     {:path [:uuid-field]
                      :data "foo"
                      :topic "bananas"
                      :clj-data (assoc valid-map :uuid-field "foo")})

    (testing "deseralization errors should contain the topic"
      (is (thrown-with-msg-and-data? clojure.lang.ExceptionInfo
                                     #"Deserialization error"
                                     {:topic "topic"}
                                     (deserialize (->serde
                                                   (json/write-str
                                                    {:type "string",
                                                     :logicalType "uuid"}))
                                                  "topic"
                                                  (serialize (->serde
                                                              (json/write-str
                                                               {:type "string"}))
                                                             "topic"
                                                             (uuid/to-string uuid/+null+))))))))

(deftest test-edn-coercion
  (testing "coercion to edn"
    (let [valid-json {"uuid_field" "00000000-0000-0000-0000-000000000000",
                      "enum_field" "a_1",
                      "optional_field" nil,
                      "bytes_field" "hello",
                      "string_field" "hello",
                      "array_field" {"array" [{"color" "yellow"}]},
                      "map_field" {"map" {"banana" {"color" "yellow"}, "ripe b4nana$" {"color" "yellow-green"}}},
                      "default_field" 1,
                      "long_field" 3,
                      "nil_field" nil}
          edn (avro/as-edn {:avro-schema complex-schema-str
                            :type-registry (merge avro/+base-schema-type-registry+
                                                  avro/+UUID-type-registry+)}
                           (json/write-str valid-json))]
      (is (= :a-1 (:enum-field edn)))
      (is (= nil (:nil-field edn)))
      (is (= {"banana" {:color "yellow"}
              "ripe b4nana$" {:color "yellow-green"}} (:map-field edn)))
      (is (= 1 (:default-field edn)))
      (is (= #uuid "00000000-0000-0000-0000-000000000000" (:uuid-field edn)))
      (is (= [{:color "yellow"}] (:array-field edn)))
      (is (= "hello" (:string-field edn)))
      (is (= nil (:optional-field edn)))
      (is (= 3 (:long-field edn)))))

  (testing "coercion to json"
    (let [valid-edn {:enum-field :a-1,
                     :bytes-field (ByteBuffer/wrap (.getBytes "hello"))
                     :nil-field nil,
                     :map-field {"banana" {:color "yellow"}, "ripe b4nana$" {:color "yellow-green"}},
                     :default-field 1,
                     :uuid-field #uuid "00000000-0000-0000-0000-000000000000",
                     :array-field [{:color "yellow"}],
                     :string-field "hello",
                     :optional-field nil,
                     :long-field 3}
          json (-> (avro/as-json {:avro-schema complex-schema-str
                                  :type-registry (merge avro/+base-schema-type-registry+
                                                        avro/+UUID-type-registry+)}
                                 valid-edn)
                   (json/read-str :key-fn keyword))]
      (is (= "a_1" (:enum_field json)))
      (is (= nil (:nil_field json)))
      (is (= {:map {:banana {:color "yellow"},
                    (keyword "ripe b4nana$") {:color "yellow-green"}}}
             (:map_field json)))
      (is (= 1 (:default_field json)))
      (is (= "00000000-0000-0000-0000-000000000000" (:uuid_field json)))
      (is (= {:array [{:color "yellow"}]}
             (:array_field json)))
      (is (= "hello" (:string_field json)))
      (is (= nil (:optional_field json)))
      (is (= 3 (:long_field json))))))



(deftest schemaless-test
  (let [serde (->serde nil)]
    (is (= (round-trip serde "bananas" "hello")
           "hello"))

    (is (= (round-trip serde "bananas" 1)
           1))

    (is (= (round-trip serde "bananas" (int 3))
           (int 3)))

    (is (= (round-trip serde "bananas" nil)
           nil))

    (is (= (round-trip serde "bananas" true)
           true))

    (is (= (round-trip serde "bananas" (float 0.34))
           (float 0.34)))

    (is (= (round-trip serde "bananas" 0.34)
           0.34))

    (is (= (String. (round-trip serde "bananas" (.getBytes "hello")))
           "hello"))

    (let [res (round-trip serde "bananas" {"hello" 3})]
      (is (instance? java.util.HashMap res))
      (is (= {(Utf8. "hello") 3} res)))))


(deftest decoupled-reader-simple-string-schema
  (let [reg-client (reg/mock-client)
        write-schema (json/write-str {:type "string"})
        read-schema nil
        write-serde (->serde write-schema reg-client)
        read-serde (->serde read-schema reg-client)]

    (is (= "yolo" (decoupled-round-trip write-serde
                                        read-serde
                                        "bananas"
                                        "yolo"
                                        identity)))))

(deftest decoupled-reader-compatible-schema
  (let [reg-client (reg/mock-client)
        write-schema (-> {:name "testRecord"
                          :type "record"
                          :fields [{:name "a"
                                    :type "string"}]}
                         json/write-str)
        read-schema (-> {:name "testRecord"
                         :type "record"
                         :fields [{:name "a"
                                   :type "string"}
                                  {:name "b"
                                   :type "string"
                                   :default "yolo"}]}
                        json/write-str)]

    (testing "use custom reader schema"
      (is (= {:a "hello"
              :b "yolo"}
             (decoupled-round-trip (->serde write-schema reg-client)
                                   (->serde read-schema reg-client {:deserializer-properties {"specific.avro.reader" true}})
                                   "bananas"
                                   {:a "hello"}
                                   identity))))))
