(ns word-count-test
  "This illustrates the use of the TopologyTestDriver and
  jackdaw.streams.mock to test Kafka Streams apps."
  (:require
   [clojure.test :refer :all]
   [jackdaw.test.fixtures :as fix]
   [jackdaw.streams.mock :as jsm]
   [word-count :as sut]))

(defn with-test-driver
  [{:keys [app topic-metadata app-config]} f]
  ;; The commented out bits are forth-coming. Put in as placeholders
  ;; for now.
  (fix/with-fixtures [#_(fix/empty-state-fixture app-config)]
    (let [topology (app topic-metadata)]
      (with-open [driver (jsm/build-driver topology #_app-config)]
        (f driver)))))

(deftest build-topology-unit-test
  (with-test-driver {:app sut/topology-builder
                     :topic-metadata sut/topic-metadata
                     :app-config sut/app-config}
    (fn [driver]
      (let [{:keys [input output]} sut/topic-metadata
            publish (partial jsm/publish driver)
            get-keyvals (partial jsm/get-keyvals driver)]

        (publish input nil "all streams lead to kafka")
        (publish input nil "hello kafka streams")

        (let [keyvals (get-keyvals output)
              counts (reduce (fn [m [k v]] (assoc m k v)) {} keyvals)]

          (is (= 8 (count keyvals)))

          (are [x k] (= x (get counts k))
            1 "all"
            2 "streams"
            1 "lead"
            1 "to"
            2 "kafka"
            1 "hello"))))))
