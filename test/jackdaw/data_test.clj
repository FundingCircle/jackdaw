(ns jackdaw.data-test
  (:require [clojure.test :refer :all]
            [jackdaw.data :as data])
  (:import org.apache.kafka.clients.producer.ProducerRecord
           [org.apache.kafka.common.header
            Headers Header]))

(set! *warn-on-reflection* false)

(deftest producer-record-arity-2
  (are [topic-config value]
      (instance? ProducerRecord
                 (data/->ProducerRecord topic-config value))
    {:topic-name "foo"} "value"
    {:topic-name "foo"} "value"))

(deftest producer-record-arity-3
  (are [topic-config key value]
      (instance? ProducerRecord
                 (data/->ProducerRecord topic-config key value))
      {:topic-name "foo"} "key" "value"
      {:topic-name "foo"} "key" "value"))

(deftest producer-record-arity-4
  (are [topic-config partition key value]
      (instance? ProducerRecord
                 (data/->ProducerRecord topic-config partition key value))
      {:topic-name "foo"} nil "key" "value"
      {:topic-name "foo"} nil "key" "value"))

(deftest producer-record-arity-5
  (are [topic-config partition timestamp key value]
      (instance? ProducerRecord
                 (data/->ProducerRecord topic-config partition timestamp key value))
      {:topic-name "foo"} nil nil "key" "value"
      {:topic-name "foo"} nil nil "key" "value"))

(deftest producer-record-arity-6
  (are [topic-config partition timestamp key value headers]
      (instance? ProducerRecord
                 (data/->ProducerRecord topic-config partition timestamp key value headers))
      {:topic-name "foo"} nil nil "key" "value" nil
      {:topic-name "foo"} nil nil "key" "value" (let [headers (map (fn [[k v]]
                                                                     (reify Header
                                                                       (key    [_] k)
                                                                       (value  [_] v)))
                                                                   [["my" "header"]])]
                                                  (reify Headers
                                                    (iterator [_]
                                                      (.iterator headers))
                                                    (spliterator [_]
                                                      (.spliterator headers))
                                                    (headers [_ key]
                                                      (filter (fn [h] (= (.key h) key))
                                                              headers))
                                                    (lastHeader [this key]
                                                      (last (.headers this key)))
                                                    (toArray [_]
                                                      (into-array Header headers))))))

(deftest map->ProducerRecord-test
  (are [m] (let [r (data/map->ProducerRecord m)]
             (and (instance? ProducerRecord r)
                  (= (.topic r)     (:topic-name m))
                  (= (.key r)       (:key m))
                  (= (.value r)     (:value m))
                  (= (.partition r) (:partition m))
                  (= (.timestamp r) (:timestamp m))))
    {:topic-name "foo"
     :value      "my string value"}
    {:topic-name "foo"
     :key        "a key"
     :value      "my string value"}
    {:topic-name "foo"
     :key        "key"
     :value      "my string value"
     :partition  nil}
    {:topic-name "foo"
     :key        "key"
     :value      "my string value"
     :partition  nil
     :timestamp  nil}
    {:topic-name "foo"
     :key        "key"
     :value      "my string value"
     :partition  nil
     :timestamp  nil
     :headers    nil}))
