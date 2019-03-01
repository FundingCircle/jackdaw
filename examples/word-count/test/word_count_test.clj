(ns word-count-test
  "This illustrates the use of the TopologyTestDriver and
  jackdaw.streams.mock to test Kafka Streams apps."
  (:require [word-count :as sut]
            [jackdaw.streams.mock :as jsm]
            [clojure.test :refer :all]))


(deftest build-topology-unit-test
  (testing "Word Count unit test"
    (let [driver (-> (sut/topology-builder sut/topic-metadata)
                     jsm/build-driver)
          publish (partial jsm/publish driver)
          get-keyvals (partial jsm/get-keyvals driver)]

      (publish (:input (sut/topic-metadata)) nil
               "all streams lead to kafka")
      (publish (:input (sut/topic-metadata)) nil
               "hello kafka streams")

      (let [keyvals (get-keyvals (:output (sut/topic-metadata)))
            counts (reduce (fn [m [k v]] (assoc m k v)) {} keyvals)]

        (is (= 8 (count keyvals)))

        (are [x k] (= x (get counts k))
          1 "all"
          2 "streams"
          1 "lead"
          1 "to"
          2 "kafka"
          1 "hello")))))
