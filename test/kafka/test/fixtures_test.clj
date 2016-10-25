(ns kafka.test.fixtures-test
  (:require
   [kafka.admin :as admin]
   [kafka.client :as client]
   [kafka.zk :as zk]
   [kafka.test.fs :as fs]
   [kafka.test.config :as config]
   [kafka.test.fixtures :as fix]
   [kafka.test.test-config :as test-config]
   [clojure.test :refer :all])
  (:import
   (org.apache.kafka.common.serialization Serdes)
   (org.apache.kafka.clients.consumer)
   (kafka.client TopicProducer TopicConsumer)))

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

(deftest find-producer-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/zookeeper)
              (fix/broker test-config/broker)
              (fix/producer-registry {:default-serde test-config/producer
                                      :custom-serde [test-config/producer long-serde str-serde]})])
        t (fn []
            (testing "default serde"
              (is (instance? TopicProducer (fix/find-producer :default-serde))))

            (testing "custom serde"
              (is (instance? TopicProducer (fix/find-producer :custom-serde)))))]
    (testing "producer publish! non-default serde"
      (fix t))))

(deftest find-consumer-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/zookeeper)
              (fix/broker test-config/broker)
              (fix/consumer-registry {:default-serde test-config/consumer
                                      :custom-serde [test-config/consumer long-serde str-serde]})])
        t (fn []
            (testing "default serde"
              (is (instance? TopicConsumer (fix/find-consumer :default-serde))))
            (testing "custom serde"
              (is (instance? TopicConsumer (fix/find-consumer :custom-serde)))))]
    (fix t)))
