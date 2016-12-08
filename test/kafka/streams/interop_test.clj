(ns kafka.streams.interop-test
  "Tests of the kafka streams wrapper."
  (:require [clojure.test :refer :all]
            [kafka.streams :as k]
            [kafka.streams.interop :as ck]
            [kafka.streams.mock :as mock]))

(deftest map-test
  (let [input-topic (mock/topic "topic-name")
        topology (-> (mock/topology-builder (ck/topology-builder))
                     (k/kstream input-topic)
                     (k/map (fn [[k v]] [v k]))
                     (mock/build))]

    (mock/send! topology input-topic 1 2)

    (let [result (mock/collect topology)]
      (is (= ["2:1"] result)))))
