(ns word-count-test
  "This illustrates the use of the TopologyTestDriver and jackdaw.test
  to test Kafka Streams topologies."
  (:require [word-count :as sut]
            [jackdaw.streams.mock :as j.streams.mock]
            [clojure.test :refer :all]))


(deftest build-topology-unit-test
  (testing "word-count unit test"
    (let [driver (j.streams.mock/build-driver sut/build-topology)
          publish (partial j.streams.mock/publish driver)
          get-keyvals (partial j.streams.mock/get-keyvals driver)]

      (publish (sut/topic-config "input") nil
               "all streams lead to kafka")

      (publish (sut/topic-config "input") nil
               "hello kafka streams")

      (let [keyvals (get-keyvals (sut/topic-config "output"))
            counts (reduce (fn [p [k v]] (assoc p k v)) {} keyvals)]

        (is (= 8 (count keyvals)))

        (are [x k] (= x (get counts k))
          1 "all"
          2 "streams"
          1 "lead"
          1 "to"
          2 "kafka"
          1 "hello")))))


(deftest build-topology-integration-test
  (testing "word-count integration test"

    ;; FIXME: Charles Reese <2018-11-18>: Create an integration test
    ;; after test machine has been added.

    ))
