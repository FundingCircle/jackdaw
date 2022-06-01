(ns jackdaw.client.partitioning-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [jackdaw.client :as client]
   [jackdaw.client.partitioning :as part]))

(set! *warn-on-reflection* false)

(deftest test-record-key->key-fn
  (let [test-key-fn (fn [key-str]
                      (-> (part/record-key->key-fn {:record-key key-str})
                          :jackdaw.client.partitioning/key-fn))]

    (testing "dollar prefix"
      (is (= 42 ((test-key-fn "$.foo") {:foo 42}))))

    (testing "hyphenated"
      (is (= 42 ((test-key-fn "foo_bar") {:foo-bar 42}))))

    (testing "dotted"
      (is (= 42 ((test-key-fn "foo.bar") {:foo {:bar 42}}))))))


(deftest test->ProducerRecord
  (with-open [p (client/producer {"bootstrap.servers" "localhost:9092"
                                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})]
    (testing "absent key-fn"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} "yolo")]
        (is (= "yolo" (.value record)))))

    (testing "identity key-fn"
      (let [record (part/->ProducerRecord p {:topic-name "foo"
                                             :key-fn identity} "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "yolo" (.key record)))))

    (testing "explicit key"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} "42" "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "42" (.key record)))))

    (testing "explicit partition"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} 1 "42" "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "42" (.key record)))
        (is (= 1 (.partition record)))))

    (testing "explicit timestamp"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} 1 0 "42" "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "42" (.key record)))
        (is (= 1 (.partition record)))
        (is (= 0 (.timestamp record)))))))

    ;; TODO how are you actually supposed to inject headers?
