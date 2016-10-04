(ns kafka.test.fixtures-test
  (:require
   [kafka.admin :as admin]
   [kafka.client :as client]
   [kafka.zk :as zk]
   [kafka.test.fs :as fs]
   [kafka.test.fixtures :as fix]
   [kafka.test.test-config :as test-config]
   [clojure.test :refer :all])
  (:import
   (org.apache.kafka.common.serialization Serdes)))

(def str-serde  (Serdes/String))
(def long-serde (Serdes/Long))  ;; clojure numbers are long by default

(deftest zookeeper-test
  (let [fix (fix/zookeeper test-config/broker)
        t (fn []
            (let [client (zk/client test-config/broker)]
              (is client)
              (.close client)))]
    (testing "zookeeper up/down"
      (fix t))))

(deftest broker-test
  (let [fix (compose-fixtures
             (fix/zookeeper test-config/broker)
             (fix/broker test-config/broker))
        t (fn []
            (let [client (zk/client test-config/broker)
                  utils (zk/utils client)]
              (is (.pathExists utils "/brokers/ids/0"))))]
    (testing "broker up/down"
      (fix t))))

(deftest producer-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/broker)
              (fix/broker test-config/broker)
              (fix/producer-registry {:words test-config/producer})])
        t (fn []
            (let [[a aa] [@(fix/publish! :words {:topic "words"
                                                 :key "1"
                                                 :value "a"})
                          @(fix/publish! :words {:topic "words"
                                                 :key "2"
                                                 :value "aa"})]]
              (is (= 1 (:serializedValueSize (client/metadata a))))
              (is (= 2 (:serializedValueSize (client/metadata aa))))))]
    (testing "producer publish!"
      (fix t))))

(deftest producer-with-serde-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/broker)
              (fix/broker test-config/broker)
              (fix/producer-registry {:words [test-config/producer
                                              (.serializer long-serde)
                                              (.serializer str-serde)]})])
        t (fn []
            (let [[a aa] [@(fix/publish! :words {:topic "words"
                                                 :key 1
                                                 :value "a"})
                          @(fix/publish! :words {:topic "words"
                                                 :key 2
                                                 :value "aa"})]]
              (is (= 1 (:serializedValueSize (client/metadata a))))
              (is (= 2 (:serializedValueSize (client/metadata aa))))))]
    (testing "producer publish! non-default serde"
      (fix t))))

(defn call-with-consumer-queue
  "Functionally consume a consumer"
  [f consumer]
  (let [latch (fix/latch 1)
        queue (fix/queue 10)
        proc (fix/consumer-loop consumer queue latch)]
    (try
      (f queue)
      (finally
        (.countDown latch)
        @proc))))

(deftest consumer-test
  (let [fix (join-fixtures
             [(fix/zookeeper test-config/broker)
              (fix/broker test-config/broker)
              (fix/consumer-registry {:words test-config/consumer})
              (fix/producer-registry {:words test-config/producer})])
        t #(call-with-consumer-queue
            (fn [queue]
              @(fix/publish! :words {:topic "words"
                                     :key "1"
                                     :value "a"})

              @(fix/publish! :words {:topic "words"
                                     :key "2"
                                     :value "aa"})

              (let [[a aa] [(.take queue)
                            (.take queue)]]
                (is (= {:topic "words"
                        :key "1"
                        :value "a"}
                       (client/select-methods a [:topic :key :value])))
                (is (= {:topic "words"
                        :key "2"
                        :value "aa"}
                       (client/select-methods aa [:topic :key :value])))))
            (fix/find-consumer :words))]
    (fix t)))
