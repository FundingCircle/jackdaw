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
                   :unique-key :id
                   :key-serde :long
                   :value-serde :edn}))

(def bar-topic
  (serde/resolver {:topic-name "bar"
                   :replication-factor 1
                   :partition-count 1
                   :unique-key :id
                   :key-serde :long
                   :value-serde :long}))

(def kafka-config {"bootstrap.servers" "localhost:9092"
                   "group.id" "kafka-write-test"})

(defn with-transport
  [t f]
  (try
    (f t)
    (finally
      (doseq [hook (:exit-hooks t)]
        (hook)))))

(deftest test-write!
  (with-transport (trns/transport {:type :kafka
                                   :config kafka-config
                                   :topics {"foo" foo-topic
                                            "bar" bar-topic}})
    (fn [t]
      (testing "valid write"
        (let [[cmd & params] [:jackdaw.test.commands/write! 1000 "foo" {:id 1 :payload "yolo"} nil]
              result (write/handle-write-cmd t cmd params)]

          (testing "returns the kafka record metadata"
            (is (= "foo" (:topic-name result)))
            (is (integer? (:offset result)))
            (is (contains? result :partition))
            (is (contains? result :serialized-key-size))
            (is (contains? result :serialized-value-size)))))

      (testing "valid write with explicit key"
        (let [[cmd & params] [:jackdaw.test.commands/write! 1000 "foo" 101 {:id 1 :payload "yolo"} nil]
              result (write/handle-write-cmd t cmd params)]

          (testing "returns the kafka record metadata"
            (is (= "foo" (:topic-name result)))
            (is (integer? (:offset result)))
            (is (contains? result :partition))
            (is (contains? result :serialized-key-size))
            (is (contains? result :serialized-value-size)))))

      (testing "invalid write"
        (testing "serialization failure"
          (let [[cmd & params] [:jackdaw.test.commands/write! 1000 "bar" {:id 1 :payload "a map is not a number"} nil]
                result (write/handle-write-cmd t cmd params)]
            (is (= :serialization-error (:error result)))
            (is (= "Cannot cast clojure.lang.PersistentArrayMap to java.lang.Long" (:message result))))))

      (testing "missing timeout"
        (let [[cmd & params] [:jackdaw.test.commands/write! "foo" {:id 1 :payload (Object.)} nil]
              result (write/handle-write-cmd t cmd params)
              problems (-> result
                           (get-in [:explain-data :clojure.spec.alpha/problems]))]

          (is (some #(.contains (:path %) :timeout) problems))))

      (testing "missing timestamp"
        (let [[cmd & params] [:jackdaw.test.commands/write! 1000 "foo" {:id 1 :payload (Object.)}]
              result (write/handle-write-cmd t cmd params)
              problems (-> result
                           (get-in [:explain-data :clojure.spec.alpha/problems]))]
          (is (some #(= (:reason %) "Insufficient input") problems)))))))
