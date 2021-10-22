(ns jackdaw.streams-test
  "Tests of the kafka streams wrapper."
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [jackdaw.serdes.edn :as jse]
            [jackdaw.streams :as k]
            [jackdaw.streams.configurable :as cfg]
            [jackdaw.streams.interop :as interop]
            [jackdaw.streams.lambdas :as lambdas :refer [key-value]]
            [jackdaw.streams.lambdas.specs]
            [jackdaw.streams.mock :as mock]
            [jackdaw.streams.protocols
             :refer [IKStream IKTable IStreamsBuilder]]
            [jackdaw.streams.specs])
  (:import [java.time Duration]
           [org.apache.kafka.streams.kstream
            JoinWindows SessionWindows TimeWindows Transformer
            ValueTransformer]
           org.apache.kafka.streams.StreamsBuilder
           [org.apache.kafka.common.serialization Serdes]))

(set! *warn-on-reflection* false)

(stest/instrument)

(deftest streams-builder
  (testing "kstream"
    (let [streams-builder (interop/streams-builder)
          kstream-a (-> streams-builder
                        (k/kstream (mock/topic "topic-a")))]

      (is (satisfies? IKStream kstream-a))))

  (testing "kstreams"
    (let [streams-builder (interop/streams-builder)
          kstream (-> streams-builder
                      (k/kstreams [(mock/topic "topic-a")
                                   (mock/topic "topic-b")]))]

      (is (satisfies? IKStream kstream))))

  (testing "ktable"
    (let [streams-builder (interop/streams-builder)
          ktable-a (-> streams-builder
                       (k/ktable (mock/topic "topic-a")))
          ktable-b (-> streams-builder
                       (k/ktable (mock/topic "topic-b") "state-store-a"))]
      (is (satisfies? IKTable ktable-a))
      (is (satisfies? IKTable ktable-b))))

  (testing "streams-builder*"
    (is (instance? StreamsBuilder
                   (k/streams-builder* (interop/streams-builder)))))

  (testing "streams-builder"
    (is (satisfies? IStreamsBuilder (k/streams-builder)))))

(defn safe-add [& args]
  (apply + (filter some? args)))

(deftest KStream
  (testing "join"
    (let [topic-a (mock/topic "table-a")
          topic-b (mock/topic "table-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/kstream builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/join left right +)
                                                    (k/to topic-c)))))]
        (let [publish-left (partial mock/publish driver topic-a)
              publish-right (partial mock/publish driver topic-b)]

          (publish-left  1 1)
          (publish-right 1 10)
          (publish-left  1 100)
          (publish-right 1 1000)
          (publish-left  1 10000)
          (publish-right 2 1)
          (publish-left  2 10)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 3 (count keyvals)))
            (is (= [1 110]   (first keyvals)))
            (is (= [1 11000] (second keyvals)))
            (is (= [2 11]    (nth keyvals 2))))))))

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


        (is (= [{:key 1
                 :value 1
                 :partition 10}] (map #(select-keys % [:key :value :partition])
                                       (mock/get-records driver
                                                         topic-b))))

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
      (is (= [1 1] ((juxt :key :value) (mock/consume driver topic-c))))
      (is (= [2 2] ((juxt :key :value) (mock/consume driver topic-c))))))

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
        (is (= [1 3] (second keyvals)))
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
        (is (= [2 3] (second keyvals)))
        (is (= [2 7] (nth keyvals 2))))))

  (testing "flat-transform"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          transformer-supplier-fn #(let [total (atom 0)]
                                     (reify Transformer
                                       (init [_ _])
                                       (close [_])
                                       (transform [_ k v]
                                         ;; each input creates two outputs
                                         ;; each v' accumulating the v read in:
                                         ;; [[k * 10, v'] [k * 20, v'']]
                                         (map (fn [x]
                                                (swap! total + v)
                                                (key-value [(* k x) @total]))
                                              [10 20]))))
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/flat-transform transformer-supplier-fn)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)
      (publish 1 4)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 6 (count keyvals)))
        (is (= [10 1] (first keyvals)))
        (is (= [20 2] (second keyvals)))
        (is (= [10 4] (nth keyvals 2)))
        (is (= [20 6] (nth keyvals 3)))
        (is (= [10 10] (nth keyvals 4)))
        (is (= [20 14] (nth keyvals 5))))))

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
        (is (= [1 3] (second keyvals)))
        (is (= [1 7] (nth keyvals 2))))))

  (testing "flat-transform-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          transformer-supplier-fn #(let [total (atom 0)]
                                    (reify ValueTransformer
                                      (init [_ _])
                                      (close [_])
                                      (transform [_ v]
                                        ;; returns value + 100,
                                        ;; then value + 200
                                        (map (fn [x]
                                               (swap! total + v)
                                               (+ @total x))
                                             [100 200]))))
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/flat-transform-values transformer-supplier-fn)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish 1 1)
      (publish 1 2)
      (publish 1 4)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 6 (count keyvals)))
        (is (= [1 101] (first keyvals)))
        (is (= [1 202] (second keyvals)))
        (is (= [1 104] (nth keyvals 2)))
        (is (= [1 206] (nth keyvals 3)))
        (is (= [1 110] (nth keyvals 4)))
        (is (= [1 214] (nth keyvals 5))))))

  (testing "kstreams"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstreams [topic-a topic-b])
                                          (k/to topic-c))))
          publish (partial mock/publish driver)]

      (publish topic-a 1 1)
      (publish topic-b 2 2)

      (is (= [[1 1] [2 2]] (mock/get-keyvals driver topic-c))))))

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
            (is (= [1 6] (nth keyvals 2))))))))

  (testing "suppress records per time window"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")

          ;; if we don't set the `grace` period of the `TimeWindows`, the
          ;; default is used: 24h - window
          window       (Duration/ofMillis 100)
          grace        (Duration/ofMillis 1)
          time-windows (-> window
                           TimeWindows/of
                           (.grace grace))]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> builder
                                                  (k/ktable topic-a)
                                                  (k/to-kstream)
                                                  (k/group-by-key)
                                                  (k/window-by-time time-windows)
                                                  (k/reduce + topic-b)
                                                  (k/suppress {})
                                                  (k/to-kstream)
                                                  (k/map (fn [[k v]] [(.key k) v]))
                                                  (k/to topic-c))))]

        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 1 3)
          (publish 2 1 4)
          (publish 201 1 1)
          (publish 202 1 2)
          (publish 303 1 1)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 2 (count keyvals)))
            (is (= [1 7] (first keyvals)))
            (is (= [1 3] (second keyvals))))))))

  (testing "suppress records with a bounded buffer"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")

          window       (Duration/ofMillis 100)
          grace        (Duration/ofMillis 1)
          time-windows (-> window
                           TimeWindows/of
                           (.grace grace))
          max-records  2]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> builder
                                                  (k/ktable topic-a)
                                                  (k/to-kstream)
                                                  (k/group-by-key)
                                                  (k/window-by-time time-windows)
                                                  (k/reduce + topic-b)
                                                  (k/suppress {:max-records max-records})
                                                  (k/to-kstream)
                                                  (k/map (fn [[k v]] [(.key k) v]))
                                                  (k/to topic-c))))]

        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 1 3)
          (publish 2 1 4)
          (publish 201 1 1)
          (publish 202 1 2)
          (publish 210 1 3)
          (publish 303 1 1)

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 2 (count keyvals)))
            (is (= [1 7] (first keyvals)))
            (is (= [1 6] (second keyvals))))))))

  (testing "suppress records with a bounded buffer that will shutdown the app"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")

          window       (Duration/ofMillis 100)
          grace        (Duration/ofMillis 1)
          time-windows (-> window
                           TimeWindows/of
                           (.grace grace))
          max-records  2]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> builder
                                                  (k/ktable topic-a)
                                                  (k/to-kstream)
                                                  (k/group-by-key)
                                                  (k/window-by-time time-windows)
                                                  (k/reduce + topic-b)
                                                  (k/suppress {:max-records max-records})
                                                  (k/to-kstream)
                                                  (k/map (fn [[k v]] [(.key k) v]))
                                                  (k/to topic-c))))]

        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 1 3)
          (publish 2 2 4)
          (try
            (publish 2 3 4)
            (catch org.apache.kafka.streams.errors.StreamsException e
              (is "task [0_0] Failed to flush state store topic-b" (.getMessage e))))

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 0 (count keyvals))))))))

  (testing "suppress records until time limit is reached"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> builder
                                                  (k/kstream topic-a)
                                                  (k/group-by-key)
                                                  (k/count topic-a)
                                                  (k/suppress {:until-time-limit-ms 10})
                                                  (k/to-kstream)
                                                  (k/to topic-b))))]

        (let [publish (partial mock/publish driver topic-a)]

          (publish 1 1 3)
          (publish 2 1 4)
          (publish 12 1 4)
          (publish 201 1 1)
          (publish 202 1 2)
          (publish 222 1 3)

          (let [keyvals (mock/get-keyvals driver topic-b)]
            (is (= 2 (count keyvals)))
            (is (= [1 3] (first keyvals)))
            (is (= [1 6] (second keyvals))))))))

  (testing "joining tables and suppressing records in between"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/ktable builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/left-join left right safe-add)
                                                    (k/suppress {:until-time-limit-ms 10})
                                                    (k/to-kstream)
                                                    (k/to topic-c)))))]

        (let [publish-left  (partial mock/publish driver topic-a)
              publish-right (partial mock/publish driver topic-b)]

          (publish-left 10 1 1)
          (publish-right 11 1 2)
          (publish-right 20 1 3) ; new time-window, will be emitted 1+3
          (publish-right 25 1 4)
          (publish-right 35 1 5) ; new time-window, will be emitted 1+5

          (let [keyvals (mock/get-keyvals driver topic-c)]
            (is (= 2 (count keyvals)))
            (is (= [1 4] (first keyvals)))
            (is (= [1 6] (second keyvals)))))))))

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

  (testing "reduce: explicit kv store"
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

  (testing "reduce: implicit kv store"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[k v]] (long (/ k 10))) topic-a)
                                          (k/reduce +)
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

  (testing "aggregate: explicit kv store"
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

  (testing "aggregate: explicit kv store with custom serdes"
    (let [serdes {:key-serde (jse/serde)
                  :value-serde (jse/serde)}
          topic-a (merge (mock/topic "topic-a") serdes)
          topic-b (merge (mock/topic "topic-b") serdes)
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[_k v]] (:uuid v)) topic-a)
                                          (k/aggregate (constantly 0)
                                                       (fn [acc [_k v]] (+ acc (:i v)))
                                                       topic-a)
                                          (k/to-kstream)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)
          uuid-a #uuid "a8e310d7-f0d6-4f81-a474-aab5d6234149"
          uuid-b #uuid "0fb9ad92-dad8-45e7-9a87-7dcd9783076e"]

      (publish uuid-a {:uuid uuid-a :i 1})
      (publish uuid-b {:uuid uuid-a :i 3})
      (publish uuid-b {:uuid uuid-b :i 5})

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= [uuid-a 1] (nth keyvals 0)))
        (is (= [uuid-a 4] (nth keyvals 1)))
        (is (= [uuid-b 5] (nth keyvals 2))))))

  (testing "aggregate: implicit kv store"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[k v]] (long (/ k 10))) topic-a)
                                          (k/aggregate (constantly -10)
                                                       (fn [acc [k v]] (+ acc v)))
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

  (testing "aggregate: diff stream"
    (let [topic-in (mock/topic "in")
          topic-diffs (mock/topic "in-diffs")
          driver (mock/build-driver (fn [builder]
                                      (let [in (-> builder
                                                   (k/kstream topic-in))]
                                        (-> in
                                            (k/group-by-key)
                                            (k/aggregate (constantly [])
                                                         (fn [acc [k v]]
                                                           (concat [(last acc)]
                                                                   [v]))
                                                         (assoc topic-in
                                                                :value-serde (jse/serde)))
                                            (k/to-kstream)
                                            (k/map (fn [[k v]]
                                                     (let [[prev current] v]
                                                       [k (when (and prev current)
                                                            (- current prev))])))
                                            (k/to topic-diffs)))))
          publish (partial mock/publish driver topic-in)]

      (publish 1 5)
      (publish 1 8)

      (let [keyvals (mock/get-keyvals driver topic-diffs)]
        (is (= 2 (count keyvals)))
        (is (= [1 nil] (first keyvals)))
        (is (= [1 3] (second keyvals))))))

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

  (testing "windowed-by-session: reduce"
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
        (is (= [1 3] (nth keyvals 4))))))

  (testing "windowed-by-session: aggregate"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by-key)
                                          (k/window-by-session (SessionWindows/with 1000))
                                          (k/aggregate (constantly 0)
                                                       (fn [agg [k v]]
                                                         (+ agg v))
                                                       ;; Merger
                                                       (fn [k agg1 agg2]
                                                         (+ agg1 agg2))
                                                       topic-a)
                                          (k/to-kstream)
                                          (k/map (fn [[k v]] [(.key k) v]))
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)]

      (publish  100 1 4)
      (publish 1000 1 3)
      (publish 1200 1 3)
      (publish 5000 1 2)
      (publish 5100 2 1)

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= 7 (count keyvals)))
        (is (= [1 4] (first keyvals)))
        (is (= [1 nil] (second keyvals)))
        (is (= [1 7] (nth keyvals 2)))
        (is (= [1 nil] (nth keyvals 3)))
        (is (= [1 10] (nth keyvals 4)))
        (is (= [1 2] (nth keyvals 5)))
        (is (= [2 1] (nth keyvals 6)))))))

(deftest grouped-table
  (testing "aggregate: explicit kv store"
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

  (testing "aggregate: explicit kv store with custom serdes"
    (let [serdes {:key-serde (jse/serde)
                  :value-serde (jse/serde)}
          topic-a (merge (mock/topic "topic-a") serdes)
          topic-b (merge (mock/topic "topic-b") serdes)
          driver (mock/build-driver (fn [builder]
                                      (-> (k/ktable builder topic-a)
                                          (k/group-by (fn [[_k v]] [(:uuid v) v]) topic-a)
                                          (k/aggregate (constantly 0)
                                                       (fn [acc [_k v]] (+ acc (:i v)))
                                                       (fn [acc [_k v]] (- acc (:i v)))
                                                       topic-b)
                                          (k/to-kstream)
                                          (k/to topic-b))))
          publish (partial mock/publish driver topic-a)
          uuid-a #uuid "a8e310d7-f0d6-4f81-a474-aab5d6234149"
          uuid-b #uuid "0fb9ad92-dad8-45e7-9a87-7dcd9783076e"]

      (publish uuid-a {:uuid uuid-a :i 1})
      (publish uuid-b {:uuid uuid-a :i 3})
      (publish uuid-b {:uuid uuid-b :i 5})

      (let [keyvals (mock/get-keyvals driver topic-b)]
        (is (= [uuid-a 1] (nth keyvals 0)))
        (is (= [uuid-a 4] (nth keyvals 1)))
        (is (= [uuid-a 1] (nth keyvals 2)))
        (is (= [uuid-b 5] (nth keyvals 3))))))

  (testing "aggregate: implicit kv store"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/group-by (fn [[k v]]
                                                                [(long (/ k 10)) v])
                                                              topic-a)
                                                  (k/aggregate (constantly 0)
                                                               (fn [acc [k v]] (+ acc v))
                                                               (fn [acc [k v]] (- acc v)))
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

  (testing "reduce: explicit kv store"
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
            (is (= [1 3] (nth keyvals 3))))))))

  (testing "reduce: implicit kv store"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/group-by (fn [[k v]]
                                                                [(long (/ k 10)) v])
                                                              topic-a)
                                                  (k/reduce + -)
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

(deftest transformer-with-ctx-test
  (testing "Value Transformer sugar with context"
    (let [input-t (merge (mock/topic "input-topic") {:value-serde (jse/serde)})
          output-t (merge (mock/topic "output-topic") {:value-serde (jse/serde)})]
      (with-open [driver (mock/build-driver
                           (fn [builder]
                             (-> (k/kstream builder input-t)
                                 (k/transform-values
                                   (lambdas/value-transformer-with-ctx
                                     (fn [ctx v]
                                       {:new-val (+ (:val v) 1)
                                        :topic (.topic ctx)})))
                                 (k/to output-t))))]
        (let [publisher (partial mock/publish driver input-t)]
          
          (publisher 100 {:val 10})

          (let [[[k v]] (mock/get-keyvals driver output-t)]
            (is (= 11 (:new-val v)))
            (is (= "input-topic" (:topic v)))))))))

(deftest with-kv-state-store-test
  (testing "Transfromer with state store sugar"
    (let [input-t (mock/topic "input-topic")
          output-t (merge (mock/topic "output-topic") {:value-serde (jse/serde)})]
      (with-open [driver (mock/build-driver
                           (fn [builder]
                             (-> builder
                                 (k/with-kv-state-store {:store-name "test-store"
                                                       :key-serde (:key-serde input-t)
                                                       :value-serde (jse/serde)})
                                 (k/kstream input-t)
                                 (k/transform
                                   (lambdas/transformer-with-ctx
                                     (fn [ctx k v]
                                       ;; Side effects on state store, procedural let ...
                                       (let [store (.getStateStore ctx "test-store")
                                             cur-val (.get store k)
                                             new-val (if-not cur-val
                                                       {:value v}
                                                       (update cur-val :value + v))]
                                         (.put store k new-val) 
                                         (key-value [k new-val]))))
                                   ["test-store"])
                                 (k/to output-t))))]
        (let [publisher (partial mock/publish driver input-t)]
          
          (publisher 1 1)
          (publisher 1 2)
          (publisher 1 3)

          (publisher 2 10)
          (publisher 2 20)
          (publisher 2 30)

          (let [msgs (into {} (mock/get-keyvals driver output-t))]
            (is (= 6 (:value (msgs 1))))
            (is (= 60 (:value (msgs 2))))))))))
