(ns echo-stream-test
  (:require [echo-stream :as sut]
            [jackdaw.streams.mock :as mock]
            [clojure.test :as t]))

(t/deftest topology-test
  (let [input (sut/topic-config "input")
        output (sut/topic-config "output")
        topology (mock/build-driver sut/build-topology)
        produce (mock/producer topology input)]

    (produce ["foo" "bar"])

    (t/is (= ["foo" "bar"]
             (mock/consume topology output)))))
