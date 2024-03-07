(ns jackdaw.serdes.json-schema.confluent-test
  (:require [jackdaw.serdes.json-schema.confluent :as jsco]
            [jackdaw.serdes.avro.schema-registry :as reg]
            [jackdaw.utils :as utils]
            [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing] :as test]))

(defn ->serde
  ([schema-str]
   (->serde schema-str (reg/mock-client)))
  ([schema-str registry-client]
   (let [serde-config (-> {:schema-registry-client registry-client}
                          ;; if no schema dont fail on invalid schema
                          (cond-> (nil? schema-str)
                            (merge {:deserializer-properties
                                    {"json.fail.invalid.schema" false}
                                    :serializer-properties
                                    {"json.fail.invalid.schema" false}})))
         schema-registry-url (utils/schema-registry-address)
         key? false]
     (jsco/serde schema-registry-url schema-str key? serde-config))))

(defn ser [serde topic x]
  (let [serializer (.serializer serde)]
    (.serialize serializer topic x)))

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

(deftest string-type-test
  (testing "String type"
    (let [schema {:type "string"}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas" "foo")
             "foo"))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" 1.1)))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" ["foo"]))))))

(deftest boolean-type-test
  (testing "Boolean type"
    (let [schema {:type "boolean"}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas" true)
             true))

      (is (= (round-trip serde "bananas" false)
             false))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" 0)))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" "true"))))))

(deftest null-type-test
  (testing "Boolean type"
    (let [schema {:type "null"}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas" nil)
             nil))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" false)))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" "")))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" 0))))))

(deftest integer-type-test
  (testing "Integer type"
    (let [schema {:type "integer"}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas" 1)
             1))

      (is (= (round-trip serde "bananas" (int 1))
             1))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" 1.1)))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" "1"))))))

(deftest number-type-test
  (testing "Number type"
    (let [schema {:type "number"}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas" 1)
             1))

      (is (= (round-trip serde "bananas" 1.1)
             1.1M))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" "1"))))))

(deftest array-type-test
  (testing "Array type"
    (let [schema
          {"$id" "https://example.com/arrays.schema.json",
           "$schema" "http://json-schema.org/draft-07/schema#",
           "description" "A representation of a person, company, organization, or place",
           "type" "array"
           "items"
           {"type" "object",
            "required" ["veggieName"
                        "veggieLike"]
            "properties"
            {"veggieName"
             {"type" "string"
              "description" "The name of the vegetable."},
             "veggieLike"
             {"type" "boolean"
              "description" "Do I like this vegetable?"}}}}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas"
                         [{"veggieName" "potato"
                           "veggieLike" true}
                          {"veggieName" "broccoli"
                           "veggieLike" false}])
             [{:veggieName "potato"
               :veggieLike true}
              {:veggieName "broccoli"
               :veggieLike false}]))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" {:array ["foo"]}))))))

(deftest object-type-test
  (testing "Object type"
    (let [schema {"type" "object"
                  "properties" {"bool" {"type" "boolean"}
                                "array" {"type" "array"
                                         "minItems" 2}}}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas" {:array ["foo" 1 {:foo "bar"}]
                                          :bool false})
             {:array ["foo" 1 {:foo "bar"}]
              :bool false}))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" {:array ["foo"]}))))))

(deftest nested-object-type-test
  (testing "Nested Object type"
    (let [schema {"type" "object"
                  "additionalProperties" false
                  "properties" {"nested"
                                {"type" "object"
                                 "additionalProperties" false
                                 "properties" {"foo" {"type" "string"}}}}}
          serde (->serde (json/write-str schema))]

      (is (= (round-trip serde "bananas" {:nested {:foo "bar"}})
             {:nested {:foo "bar"}}))

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Error serializing JSON message"
                            (round-trip serde "bananas" {:object {:foo 1}}))))))

(deftest schemaless-test
  (testing "a nil schema with disabled validation"
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
             0.34M))

      (is (= (round-trip serde "bananas" 0.34)
             0.34M))

      (is (= (round-trip serde "bananas" 0.34M)
             0.34M))

      (let [res (round-trip serde "bananas" {:hello 3})]
        (is (= {:hello 3} res)))

      (let [res (round-trip serde "bananas" {"hello" 3})]
        (is (= {:hello 3} res))))))

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
