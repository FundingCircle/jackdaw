(ns kafka.test.fixtures-test
  (:require
   [clj-http.client :as http]
   [clojure.data.json :as json]
   [clojure.string :as str]
   [kafka.admin :as admin]
   [kafka.client :as client]
   [kafka.zk :as zk]
   [kafka.test.fs :as fs]
   [kafka.test.config :as config]
   [kafka.test.fixtures :as fix]
   [kafka.test.test-config :as test-config]
   [clojure.test :refer :all])
  (:import
   (org.apache.kafka.common.serialization Serdes)))

(def str-serde  (Serdes/String))
(def long-serde (Serdes/Long))  ;; clojure numbers are long by default

(deftest zookeeper-test
  (let [fix (fix/zookeeper test-config/zookeeper)
        t (fn []
            (let [client (zk/client test-config/broker)]
              (is client)
              (.close client)))]
    (testing "zookeeper up/down"
      (fix t))))

(deftest broker-test
  (let [fix (compose-fixtures
             (fix/zookeeper test-config/zookeeper)
             (fix/broker test-config/broker))
        t (fn []
            (let [client (zk/client test-config/broker)
                  utils (zk/utils client)]
              (is (.pathExists utils "/brokers/ids/0"))))]
    (testing "broker up/down"
      (fix t))))

(defn- schema-registry-tests* []
  (let [test-url (fn [& path]
                   (format "%s/%s"
                           (get test-config/schema-registry "listeners")
                           (str/join "/" (map (comp str name) path))))]

    ;; don't think there's much point in extensive test of schema registry functionality
    ;; here but I do think it's worth just creating a few schemas to prove connectivity
    ;; with any other fixtures being used (particularly the multi-broker)
    ;;
    ;; the tests below assume a schema registry is running using the test config and
    ;; verifies the functionality described in the confluent quickstart

    (testing "basic healthcheck"
      (let [result (http/get (test-url))]
        (is (= 200 (:status result)))
        (is (= "application/vnd.schemaregistry.v1+json" (get-in result [:headers "Content-Type"])))))

    (testing "create key schema"
      (let [result (http/post (test-url :subjects :kafka-key :versions)
                              {:headers {"Content-Type" "application/vnd.schemaregistry.v1+json"}
                               :body (json/write-str {:schema (json/write-str {:type "string"})})})]
        (is (= 200 (:status result)))
        (is (= {"id" 1}
               (json/read-str (:body result))))))

    (testing "create value schema"
      (let [result (http/post (test-url :subjects :kafka-value :versions)
                              {:headers {"Content-Type" "application/vnd.schemaregistry.v1+json"}
                               :body (json/write-str {:schema (json/write-str {:type "string"})})})]
        (is (= 200 (:status result)))
        (is (= {"id" 1}
               (json/read-str (:body result))))))

    (testing "list subjects"
      (let [result (http/get (test-url :subjects)
                             {:headers {"Content-Type" "application/vnd.schemaregistry.v1+json"}})]
        (is (= 200 (:status result)))
        (is (= (set ["kafka-value", "kafka-key"])
               (set (json/read-str (:body result)))))))

    (testing "list versions by subject"
      (let [result (http/get (test-url :subjects :kafka-value :versions)
                             {:headers {"Content-Type" "application/vnd.schemaregistry.v1+json"}})]
        (is (= 200 (:status result)))
        (is (= [1] (json/read-str (:body result))))))

    (testing "fetch schema by id"
      (let [result (http/get (test-url :schemas :ids "1")
                             {:headers {"Content-Type" "application/vnd.schemaregistry.v1+json"}})]
        (is (= {"schema" (json/write-str "string")}
               (json/read-str (:body result))))))


    (testing "fetch schema by subject and version"
      (let [result (http/get (test-url :subjects :kafka-value :versions "1")
                             {:headers {"Content-Type" "application/vnd.schemaregistry.v1+json"}})]
        (is (= {"subject" "kafka-value"
                "version" 1
                "id" 1
                "schema" (json/write-str "string")}
               (json/read-str (:body result))))))

    ;; seems legit

    ))

(deftest schema-registry-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/broker)
              (fix/broker test-config/broker)
              (fix/schema-registry test-config/schema-registry)])]

    (testing "schema registry"
      (fix schema-registry-tests*))))

(deftest multi-broker-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/zookeeper)
              (fix/multi-broker test-config/broker 3)])
        t (fn []
            (with-open [client (zk/client test-config/broker)]
              (let [utils (zk/utils client)]
                (is (.pathExists utils "/brokers/ids/0"))
                (is (.pathExists utils "/brokers/ids/1"))
                (is (.pathExists utils "/brokers/ids/2")))))]
    (testing "multi-broker fixture"
      (fix t))))

(deftest schema-registry-with-multi-broker-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/broker)
              (fix/multi-broker test-config/broker 3)
              (fix/schema-registry test-config/schema-registry)])]
    (testing "schema registry with multi broker"
      (fix schema-registry-tests*))))
