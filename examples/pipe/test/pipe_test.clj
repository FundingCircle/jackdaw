(ns pipe-test
  "This illustrates the use of the TopologyTestDriver and jackdaw.test
  to test Kafka Streams topologies."
  (:require [pipe :as sut]
            [jackdaw.streams.mock :as j.streams.mock]
            [clojure.test :refer :all]))


(deftest build-topology-unit-test
  (testing "pipe unit test"
    (let [driver (j.streams.mock/build-driver sut/build-topology)
          publish (partial j.streams.mock/publish driver)
          get-keyvals (partial j.streams.mock/get-keyvals driver)]

      (publish (sut/topic-config "input") nil "this is a pipe")

      (let [keyvals (get-keyvals (sut/topic-config "output"))]
        (is (= 1 (count keyvals)))
        (is (= [nil "this is a pipe"] (first keyvals)))))))


(deftest build-topology-integration-test
  (testing "pipe integration test"

    ;; FIXME: Charles Reese <2018-11-24>: Create an integration test
    ;; after test machine has been added.

    ))
