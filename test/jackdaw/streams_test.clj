(ns jackdaw.streams-test
  "Tests of the kafka streams wrapper."
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [jackdaw.streams :as k]
            [jackdaw.streams.configurable :as cfg]
            [jackdaw.streams.lambdas :as lambdas :refer [key-value]]
            [jackdaw.streams.lambdas.specs]
            [jackdaw.streams.mock :as mock]
            [jackdaw.streams.protocols
             :refer [IKStream IKTable IStreamsBuilder]]
            [jackdaw.streams.specs])
  (:import [org.apache.kafka.streams.kstream
            JoinWindows SessionWindows TimeWindows Transformer
            ValueTransformer]
           org.apache.kafka.streams.StreamsBuilder
           [org.apache.kafka.common.serialization Serdes]))

(stest/instrument)

(defn close-test-driver [cfg-topology]
  (-> cfg-topology
      (cfg/config)
      (:jackdaw.streams.mock/test-driver)
      (.close)))

(deftest streams-builder
  (testing "kstream"
    (let [streams-builder (mock/streams-builder)
          kstream-a (-> streams-builder
                        (k/kstream (mock/topic "topic-a")))]

      (is (satisfies? IKStream kstream-a))))

  (testing "kstreams"
    (let [streams-builder (mock/streams-builder)
          kstream (-> streams-builder
                      (k/kstreams [(mock/topic "topic-a")
                                   (mock/topic "topic-b")]))]

      (is (satisfies? IKStream kstream))))

  (testing "ktable"
    (let [streams-builder (mock/streams-builder)
          ktable-a (-> streams-builder
                       (k/ktable (mock/topic "topic-a")))]
      (is (satisfies? IKTable ktable-a))))

  (testing "streams-builder*"
    (is (instance? StreamsBuilder
                   (k/streams-builder* (mock/streams-builder)))))

  (testing "streams-builder"
    (is (satisfies? IStreamsBuilder (k/streams-builder)))))

(defn safe-add [& args]
  (apply + (filter some? args)))

(deftest KStream
  (testing "left-join"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/kstream builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (k/to (k/left-join left right safe-add) topic-c))))]
        (let [publish-left (partial mock/publish driver topic-a)
              publish-right (partial mock/publish driver topic-b)]

          (publish-left 1 2) ;; table: nil, event: 2
          (publish-right 1 1) ;; Add to table
          (publish-left 1 2) ;; table: 1, event: 2

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= [1 2] (first keyvals)))
            (is (= [1 3] (second keyvals)))))))


    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/kstream builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (k/to (k/left-join left right safe-add topic-a topic-b) topic-c))))]
        (let [publish-left (partial mock/publish driver topic-a)
              publish-right (partial mock/publish driver topic-b)]

          (publish-left 1 2)
          (publish-right 1 1)
          (publish-left 1 2)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= [1 2] (first keyvals)))
            (is (= [1 3] (second keyvals))))))))

  (testing "for-each!"
    (let [topic-a (mock/topic "topic-a")
          sentinel (atom [])
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/for-each! (fn [[_ x]] (swap! sentinel conj x))))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)

      (is (= [1 2]
             @sentinel))))

  (testing "filter"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/filter (fn [[k v]] (> v 1)))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)

      (is (= [[1 2]] (mock/get-keyvals driver topic-b)))))

  (testing "filter-not"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/filter-not (fn [[k v]] (> v 1)))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)

      (is (= [[1 1]] (mock/get-keyvals driver topic-b)))))

  (testing "map-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/map-values inc)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= [1 2] (first keyvals)))
        (is (= [1 3] (second keyvals))))))

  (testing "peek"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          sentinel (atom [])
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/peek (fn [[_ x]] (swap! sentinel conj x)))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)

      (is (= [1 2]
             @sentinel))))

  ;; Per KAFKA-7326 Kstream.print() does not automatically flush so
  ;; there is no good way to rest this until fix version 2.1.0
  ;; https://issues.apache.org/jira/browse/KAFKA-7326
  #_(testing "print!"
    (let [std-out System/out
          mock-out (java.io.ByteArrayOutputStream.)]

      (try
        (System/setOut (java.io.PrintStream. mock-out))
        (let [topic-a (mock/topic "topic-a")
              driver (mock/build-driver (fn [builder]
                                          (-> builder
                                              (k/kstream topic-a)
                                              (k/print!))))
              publish (partial mock/publish driver topic-a)]
          (publish 1 1)
          (is (= "[KSTREAM-SOURCE-0000000000]: 1, 2\n" (.toString mock-out))))
        (finally
          (System/setOut std-out)))))

  (testing "through"
    (testing "without partitions"
      (let [topic-a (mock/topic "topic-a")
            topic-b (mock/topic "topic-b")
            topic-c (mock/topic "topic-c")
            driver (mock/build-driver (fn [builder]
                                        (-> builder
                                            (k/kstream topic-a)
                                            (k/through topic-b)
                                            (k/to topic-c))))
            publish (partial mock/publish driver topic-a)]

        (publish 1 1)

        (is (= [[1 1]] (mock/get-keyvals driver topic-b)))
        (is (= [[1 1]] (mock/get-keyvals driver topic-c)))))
    (testing "with partition"
      (let [topic-a (mock/topic "topic-a")
            topic-b (assoc (mock/topic "topic-b") :partition-fn (fn [topic-name key value partition-count]
                                                                  (int 10)))
            topic-c (mock/topic "topic-c")
            driver (mock/build-driver (fn [builder]
                                        (-> builder
                                            (k/kstream topic-a)
                                            (k/through topic-b)
                                            (k/to topic-c))))
            publish (partial mock/publish driver topic-a)]

        (publish 1 1)

        (let [opts {:extractor mock/datafying-extractor}]
          (is (= [{:key 1
                 :value 1
                 :partition 10}] (map #(select-keys % [:key :value :partition])
                                        (mock/get-keyvals driver
                                                          topic-b
                                                          opts)))))

        (is (= [[1 1]] (mock/get-keyvals driver topic-c))))))


  (testing "to"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)

      (is (= [[1 1]] (mock/get-keyvals driver topic-b)))))

  (testing "branch"
    (let [topic-a (mock/topic "topic-a")
          topic-pos (mock/topic "topic-pos")
          topic-neg (mock/topic "topic-neg")
          driver (mock/build-driver (fn [builder]
                                      (let [[pos-stream neg-stream] (-> builder
                                                                        (k/kstream topic-a)
                                                                        (k/branch [(fn [[k v]]
                                                                                     (<= 0 v))
                                                                                   (constantly true)]))]
                                        (k/to pos-stream topic-pos)
                                        (k/to neg-stream topic-neg))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 -1)

      (is (= [[1 1]] (mock/get-keyvals driver topic-pos)))
      (is (= [[1 -1]] (mock/get-keyvals driver topic-neg)))))

  (testing "flat-map"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/flat-map (fn [[k v]]
                                                        [[k v]
                                                         [k 0]]))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= [1 1] (first keyvals)))
        (is (= [1 0] (second keyvals))))))

  (testing "flat-map-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/flat-map-values (fn [v]
                                                               [v (inc v)]))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= [1 1] (first keyvals)))
        (is (= [1 2] (second keyvals))))))

  (testing "join-windowed"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")
          windows (JoinWindows/of 1000)
          driver (mock/build-driver (fn [builder]
                                      (let [left-kstream (k/kstream builder topic-a)
                                            right-kstream (k/kstream builder topic-b)]
                                        (-> left-kstream
                                            (k/join-windowed right-kstream
                                                             +
                                                             windows
                                                             topic-a
                                                             topic-b)
                                            (k/to topic-c)))))
          publish-a (partial mock/publish driver topic-a)
          publish-b (partial mock/publish driver topic-b)]

      (publish-a 1 1 1)
      (publish-b 100 1 2)
      (publish-b 10000 1 4) ;; Outside of join window
      (is (= [[1 3]] (mock/get-keyvals driver topic-c)))))

  (testing "map"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/map (fn [[k v]]
                                                   [(inc k) (inc v)]))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)

      (is (= [[2 2]] (mock/get-keyvals driver topic-b)))))

  (testing "merge"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/merge (k/kstream builder topic-b))
                                          (k/to topic-c))))
          produce-a (mock/producer driver topic-a)
          produce-b (mock/producer driver topic-b)]

      (produce-a 1 1 1)
      (produce-b 100 2 2)
      (is (= [1 1] (mock/consume driver topic-c mock/producer-record)))
      (is (= [2 2] (mock/consume driver topic-c mock/producer-record)))))

  (testing "outer-join-windowed"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")
          windows (JoinWindows/of 1000)
          driver (mock/build-driver (fn [builder]
                                      (let [left-kstream (k/kstream builder topic-a)
                                            right-kstream (k/kstream builder topic-b)]
                                        (-> left-kstream
                                            (k/outer-join-windowed right-kstream
                                                                   safe-add
                                                                   windows
                                                                   topic-a
                                                                   topic-b)
                                            (k/to topic-c)))))
          publish-a (partial mock/publish driver topic-a)
          publish-b (partial mock/publish driver topic-b)]

      (publish-a 1 1 1)
      (publish-b 100 1 2)
      (publish-b 10000 1 4) ;; Outside of join window

      (let [keyvals (mock/get-keyvals driver topic-c)]
        (is (= 3 (count keyvals)))
        (is (= [1 1] (first keyvals)))
        (is (= [1 3] (second keyvals )))
        (is (= [1 4] (nth keyvals 2))))))

  (testing "process!"
    (let [topic-a (mock/topic "topic-a")
          records (atom [])
          driver (mock/build-driver (fn [builder]
                                      (-> (k/kstream builder topic-a)
                                          (k/process! (fn [ctx k v]
                                                        (swap! records conj v))
                                                      []))))
          publish-a (partial mock/publish driver topic-a)]

      (publish-a 1 1 1)
      (is (= [1] @records))))

  (testing "select-key"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/select-key (fn [[k v]]
                                                          (inc k)))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)

      (is (= [[2 1]] (mock/get-keyvals driver topic-b)))))

  (testing "transform"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          transformer-supplier-fn #(let [total (atom 0)]
                                    (reify Transformer
                                      (init [_ _])
                                      (close [_])
                                      (transform [_ k v]
                                        (swap! total + v)
                                        (key-value [(* k 2) @total]))))
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/transform transformer-supplier-fn)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)
      (publish 1 4)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= [2 1] (first keyvals)))
        (is (= [2 3] (second keyvals )))
        (is (= [2 7] (nth keyvals 2))))))

  (testing "transform-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          transformer-supplier-fn #(let [total (atom 0)]
                                    (reify ValueTransformer
                                      (init [_ _])
                                      (close [_])
                                      (transform [_ v]
                                        (swap! total + v)
                                        @total)))
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/transform-values transformer-supplier-fn)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)
      (publish 1 4)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 3 (count keyvals)))
        (is (= [1 1] (first keyvals)))
        (is (= [1 3] (second keyvals )))
        (is (= [1 7] (nth keyvals 2)))))))

(deftest KTable
  (testing "filter"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/filter (fn [[k v]]
                                                              (not (zero? v))))
                                                  (k/to-kstream)
                                                  (k/to topic-b))))]
        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 2)
          (publish 1 0)

          (let [keyvals (mock/get-keyvals driver topic-b)]
            (is (= 2 (count keyvals)))
            (is (= [1 2] (first keyvals)))
            (is (= [1 nil] (second keyvals)))))))) ;; Tombstone from filter

  (testing "filter-not"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/filter-not (fn [[k v]]
                                                                  (not (zero? v))))
                                                  (k/to-kstream)
                                                  (k/to topic-b))))]
        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 0)
          (publish 1 2)

          (let [keyvals (mock/get-keyvals driver topic-b)]
            (is (= 2 (count keyvals)))
            (is (= [1 0] (first keyvals)))
            (is (= [1 nil] (second keyvals)))))))) ;; Tombstone from filter

  (testing "map-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/map-values (fn [v]
                                                                  (inc v)))
                                                  (k/to-kstream)
                                                  (k/to topic-b))))]
        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 0)
          (publish 1 2)
          (publish 2 0)

          (let [keyvals (mock/get-keyvals driver topic-b)]
            (is (= 3 (count keyvals)))
            (is (= [1 1] (first keyvals)))
            (is (= [1 3] (second keyvals)))
            (is (= [2 1] (nth keyvals 2))))))))

  (testing "group-by"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/group-by (fn [[k v]]
                                                                [(if (even? k) k (inc k)) v])
                                                              topic-a)
                                                  (k/count topic-b)
                                                  (k/to-kstream)
                                                  (k/to topic-c))))]
        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 0)
          (publish 2 0)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 2 (count keyvals)))
            (is (= [2 1] (first keyvals)))
            (is (= [2 2] (second keyvals))))))))

  (testing "join"
    (let [topic-a (mock/topic "table-a")
          topic-b (mock/topic "table-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/ktable builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/join left right +)
                                                    (k/to-kstream)
                                                    (k/to topic-c)))))]
        (let [publish-left (partial mock/publish driver topic-a)
              publish-right (partial mock/publish driver topic-b)]

          (publish-left 1 1)
          (publish-right 1 2)
          (publish-left 1 4)
          (publish-left 2 42)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 2 (count keyvals)))
            (is (= [1 3] (first keyvals)))
            (is (= [1 6] (second keyvals))))))))

  (testing "outer-join"
    (let [topic-a (mock/topic "table-a")
          topic-b (mock/topic "table-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/ktable builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/outer-join left right safe-add)
                                                    (k/to-kstream)
                                                    (k/to topic-c)))))]
        (let [publish-left (partial mock/publish driver topic-a)
              publish-right (partial mock/publish driver topic-b)]

          (publish-left 1 1)
          (publish-right 1 2)
          (publish-left 1 4)
          (publish-left 2 42)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 4 (count keyvals)))
            (is (= [1 1] (first keyvals)))
            (is (= [1 3] (second keyvals)))
            (is (= [1 6] (nth keyvals 2)))
            (is (= [2 42] (nth keyvals 3))))))))

  (testing "left-join"
    (let [topic-a (mock/topic "table-a")
          topic-b (mock/topic "table-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/ktable builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/left-join left right safe-add)
                                                    (k/to-kstream)
                                                    (k/to topic-c)))))]
        (let [publish-left (partial mock/publish driver topic-a)
              publish-right (partial mock/publish driver topic-b)]

          (publish-left 1 1)
          (publish-right 1 2)
          (publish-left 1 4)
          (publish-right 2 42)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 3 (count keyvals)))
            (is (= [1 1] (first keyvals)))
            (is (= [1 3] (second keyvals)))
            (is (= [1 6] (nth keyvals 2)))))))))

(deftest grouped-stream
  (testing "count"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by-key)
                                          (k/count topic-a)
                                          (k/to-kstream)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 2 (count keyvals)))
        (is (= [1 1] (first keyvals)))
        (is (= [1 2] (last keyvals))))))

  (testing "reduce"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[k v]] (long (/ k 10))) topic-a)
                                          (k/reduce + topic-a)
                                          (k/to-kstream)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)
      (publish 10 2)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 3 (count keyvals)))
        (is (= [0 1] (first keyvals)))
        (is (= [0 3] (second keyvals)))
        (is (= [1 2] (nth keyvals 2))))))

  (testing "aggregate"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[k v]] (long (/ k 10))) topic-a)
                                          (k/aggregate (constantly -10)
                                                       (fn [acc [k v]] (+ acc v))
                                                       topic-a)
                                          (k/to-kstream)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)
      (publish 10 2)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 3 (count keyvals)))
        (is (= [0 -9] (first keyvals)))
        (is (= [0 -7] (second keyvals)))
        (is (= [1 -8] (nth keyvals 2))))))

  (testing "windowed-by-time"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[k v]] (long (/ k 10))) topic-a)
                                          (k/window-by-time (TimeWindows/of 1000))
                                          (k/reduce + topic-a)
                                          (k/to-kstream)
                                          (k/map (fn [[k v]] [(.key k) v]))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1000 1 1)
      (publish 1500 1 2)
      (publish 5000 1 4)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 3 (count keyvals)))
        (is (= [0 1] (first keyvals)))
        (is (= [0 3] (second keyvals)))
        (is (= [0 4] (nth keyvals 2))))))

  (testing "windowed-by-time with string keys"
    (let [topic-a (assoc (mock/topic "topic-a") :key-serde (Serdes/String))
          topic-b (assoc (mock/topic "topic-b") :key-serde (Serdes/String))
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by-key)
                                          (k/window-by-time (TimeWindows/of 1000))
                                          (k/reduce + topic-a)
                                          (k/to-kstream)
                                          (k/map (fn [[k v]] [(.key k) v]))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1000 "a" 1)
      (publish 1500 "a" 2)
      (publish 5000 "a" 4)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 3 (count keyvals)))
        (is (= ["a" 1] (first keyvals)))
        (is (= ["a" 3] (second keyvals)))
        (is (= ["a" 4] (nth keyvals 2))))))

  (testing "windowed-by-session"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[k v]] (long (/ k 10))) topic-a)
                                          (k/window-by-session (SessionWindows/with 1000))
                                          (k/reduce + topic-a)
                                          (k/to-kstream)
                                          (k/map (fn [[k v]] [(.key k) v]))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1000 1 1)
      (publish 1500 1 2)
      (publish 5000 1 3)
      (publish 5100 12 3)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 5 (count keyvals)))
        (is (= [0 1] (first keyvals)))
        (is (= [0 nil] (second keyvals)))
        (is (= [0 3] (nth keyvals 2)))
        (is (= [0 3] (nth keyvals 3)))
        (is (= [1 3] (nth keyvals 4)))))))

(deftest grouped-table
  (testing "aggregate"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/group-by (fn [[k v]]
                                                                [(long (/ k 10)) v])
                                                              topic-a)
                                                  (k/aggregate (constantly 0)
                                                               (fn [acc [k v]] (+ acc v))
                                                               (fn [acc [k v]] (- acc v))
                                                               topic-b)
                                                  (k/to-kstream)
                                                  (k/to topic-b))))]
        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 1)
          (publish 2 2)
          (publish 2 nil)
          (publish 10 3)

          (let [keyvals (mock/get-keyvals driver topic-b)]
            (is (= 4 (count keyvals)))
            (is (= [0 1] (first keyvals)))
            (is (= [0 3] (second keyvals)))
            (is (= [0 1] (nth keyvals 2)))
            (is (= [1 3] (nth keyvals 3))))))))

  (testing "count"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/group-by (fn [[k v]]
                                                                [(if (even? k) k (inc k)) v])
                                                              topic-a)
                                                  (k/count topic-b)
                                                  (k/to-kstream)
                                                  (k/to topic-c))))]
        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 0)
          (publish 2 0)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 2 (count keyvals)))
            (is (= [2 1] (first keyvals)))
            (is (= [2 2] (second keyvals))))))))

  (testing "reduce"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/group-by (fn [[k v]]
                                                                [(long (/ k 10)) v])
                                                              topic-a)
                                                  (k/reduce + - topic-b)
                                                  (k/to-kstream)
                                                  (k/to topic-b))))]
        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 1)
          (publish 2 2)
          (publish 2 nil)
          (publish 10 3)

          (let [keyvals (mock/get-keyvals driver topic-b)]
            (is (= 4 (count keyvals)))
            (is (= [0 1] (first keyvals)))
            (is (= [0 3] (second keyvals)))
            (is (= [0 1] (nth keyvals 2)))
            (is (= [1 3] (nth keyvals 3)))))))))

(deftest GlobalKTableTest
  (testing "inner global join"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [k-stream (k/kstream builder topic-a)
                                                    k-table (k/global-ktable builder topic-b)]
                                                (-> k-stream
                                                    (k/join-global k-table
                                                                   (fn [[k v]]
                                                                     k)
                                                                   +)
                                                    (k/to topic-c)))))]
        (let [publish-stream (partial mock/publish driver topic-a)
              publish-table (partial mock/publish driver topic-b)]

          (publish-stream 1 1)
          (publish-table 1 2)
          (publish-stream 1 4)

          (is (= [[1 6]] (mock/get-keyvals driver topic-c)))))))

  (testing "left global join"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [k-stream (k/kstream builder topic-a)
                                                    k-table (k/global-ktable builder topic-b)]
                                                (-> k-stream
                                                    (k/left-join-global k-table
                                                                        (fn [[k v]]
                                                                          k)
                                                                        safe-add)
                                                    (k/to topic-c)))))]
        (let [publish-stream (partial mock/publish driver topic-a)
              publish-table (partial mock/publish driver topic-b)]

          (publish-stream 1 1)
          (publish-table 1 2)
          (publish-stream 1 4)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 2 (count keyvals)))
            (is (= [1 1] (first keyvals)))
            (is (= [1 6] (second keyvals)))))))))

