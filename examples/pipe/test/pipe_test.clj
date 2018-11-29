(ns pipe-test
  "This illustrates the use of the TopologyTestDriver and jackdaw.test
  to test Kafka Streams topologies."
  (:require [pipe :as sut]
            [jackdaw.streams.mock :as jsm]
            [clojure.test :refer :all]))


(deftest build-topology-unit-test
  (testing "pipe unit test"
    (let [driver (jsm/build-driver sut/build-topology)
          publish (partial jsm/publish driver)
          get-keyvals (partial jsm/get-keyvals driver)]

      (publish (sut/topic-config "input") nil "this is a pipe")

      (let [keyvals (get-keyvals (sut/topic-config "output"))]
        (is (= 1 (count keyvals)))
        (is (= [nil "this is a pipe"] (first keyvals)))))))
