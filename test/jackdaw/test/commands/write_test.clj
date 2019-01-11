(ns jackdaw.test.commands.write-test
  (:require
   [jackdaw.test.commands.write :as write]
   [jackdaw.test.transports :as trns]
   [jackdaw.test.transports.kafka]
   [jackdaw.test.serde :as serde]
   [jackdaw.test :refer [test-machine]]
   [clojure.test :refer :all]))

(def foo-topic
  (serde/resolver {:topic-name "foo"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :long
                   :value-serde :edn}))

(def bar-topic
  (serde/resolver {:topic-name "bar"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :long
                   :value-serde :long}))

(def baz-topic
  (serde/resolver {:topic-name "baz"
                   :replication-factor 1
                   :partition-count 5
                   :key-serde :long
                   :value-serde :json}))

(def baz2-topic
  (serde/resolver {:topic-name "baz2"
                   :replication-factor 1
                   :partition-count 5
                   :key-fn :id2
                   :partition-fn (constantly 100)
                   :key-serde :long
                   :value-serde :json}))

(def kafka-config {"bootstrap.servers" "localhost:9092"
                   "group.id" "kafka-write-test"})

(defn with-transport
  [t f]
  (try
    (f t)
    (finally
      (doseq [hook (:exit-hooks t)]
        (hook)))))

(deftest test-create-message
  (testing "create a message to send"
    (let [input-msg {:id 3 :id2 100 :id3 3000 :payload "yolo"}
          prepared-msg-1 (write/create-message baz-topic input-msg {})
          prepared-msg-2 (write/create-message baz-topic input-msg {:key 1234})
          prepared-msg-3 (write/create-message baz2-topic input-msg {})
          prepared-msg-4 (write/create-message baz-topic input-msg
                                               {:key-fn :id2
                                                :partition-fn (constantly 200)})
          prepared-msg-5 (write/create-message baz2-topic input-msg
                                               {:key-fn :id3
                                                :partition-fn (constantly 300)})
          prepared-msg-6 (write/create-message baz-topic input-msg
                                               {:key 1000000
                                                :partition-fn (constantly 400)})
          prepared-msg-7 (write/create-message baz-topic input-msg {:partition 777})
          prepared-msg-8 (write/create-message baz-topic input-msg {:key 1234
                                                                    :partition 777})]
      (is (= 3 (:key prepared-msg-1)))
      (is (= 4 (:partition prepared-msg-1)))

      (is (= 1234 (:key prepared-msg-2)))
      (is (= 2 (:partition prepared-msg-2)))

      (is (= 100 (:key prepared-msg-3)))
      (is (= 100 (:partition prepared-msg-3)))

      (is (= 100 (:key prepared-msg-4)))
      (is (= 200 (:partition prepared-msg-4)))

      (is (= 3000 (:key prepared-msg-5)))
      (is (= 300 (:partition prepared-msg-5)))

      (is (= 1000000 (:key prepared-msg-6)))
      (is (= 400 (:partition prepared-msg-6)))

      (is (= 3 (:key prepared-msg-7)))
      (is (= 777 (:partition prepared-msg-7)))

      (is (= 1234 (:key prepared-msg-8)))
      (is (= 777 (:partition prepared-msg-8))))))

(deftest test-write!
  (with-transport (trns/transport {:type :kafka
                                   :config kafka-config
                                   :topics {"foo" foo-topic
                                            "bar" bar-topic}})
    (fn [t]
      (testing "valid write"
        (let [[cmd & params] [:write! "foo" {:id 1 :payload "yolo"}]
              result (write/handle-write-cmd t cmd params)]

          (testing "returns the kafka record metadata"
            (is (= "foo" (:topic-name result)))
            (is (integer? (:offset result)))
            (is (contains? result :partition))
            (is (contains? result :serialized-key-size))
            (is (contains? result :serialized-value-size)))))

      (testing "valid write with explicit key"
        (let [[cmd & params] [:write! "foo" {:id 1 :payload "yolo"} {:key 101}]
              result (write/handle-write-cmd t cmd params)]

          (testing "returns the kafka record metadata"
            (is (= "foo" (:topic-name result)))
            (is (integer? (:offset result)))
            (is (contains? result :partition))
            (is (contains? result :serialized-key-size))
            (is (contains? result :serialized-value-size)))))

      (testing "invalid write"
        (testing "serialization failure"
          (let [[cmd & params] [:write! "bar" {:id 1 :payload "a map is not a number"}]
                result (write/handle-write-cmd t cmd params)]
            (is (= :serialization-error (:error result)))
            (is (= "Cannot cast clojure.lang.PersistentArrayMap to java.lang.Long" (:message result)))))))))
