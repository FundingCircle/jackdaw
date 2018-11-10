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
            [jackdaw.streams.protocols :refer [IKStream IKTable IStreamsBuilder]]
            [jackdaw.streams.specs])
  (:import [org.apache.kafka.streams.kstream JoinWindows SessionWindows TimeWindows Transformer ValueTransformer]
           org.apache.kafka.streams.StreamsBuilder))

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
                                                (k/to! (k/left-join left right safe-add) topic-c))))]
        (let [produce-left (mock/producer driver topic-a)
              produce-right (mock/producer driver topic-b)]

          (produce-left [1 2]) ;; table: nil, event: 2
          (produce-right [1 1]) ;; Add to table
          (produce-left [1 2]) ;; table: 1, event: 2

          (is (= [1 2]
                 (mock/consume driver topic-c)))
          (is (= [1 3]
                 (mock/consume driver topic-c)))))))

  (testing "for-each!"
    (let [topic-a (mock/topic "topic-a")
          sentinel (atom [])
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/for-each! (fn [[_ x]] (swap! sentinel conj x))))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])

      (is (= [1 2]
             @sentinel))))

  (testing "filter"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/filter (fn [[k v]] (> v 1)))
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])

      (let [result (mock/consume driver topic-b)]
        (is (= result [1 2])))))

  (testing "filter-not"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/filter-not (fn [[k v]] (> v 1)))
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])

      (let [result (mock/consume driver topic-b)]
        (is (= result [1 1])))))

  (testing "map-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/map-values inc)
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])

      (is (= (mock/consume driver topic-b) [1 2]))
      (is (= (mock/consume driver topic-b) [1 3]))))

  (testing "peek"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          sentinel (atom [])
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/peek (fn [[_ x]] (swap! sentinel conj x)))
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])

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
              produce (mock/producer driver topic-a)]
          (produce [1 1])
          (is (= "[KSTREAM-SOURCE-0000000000]: 1, 2\n" (.toString mock-out))))
        (finally
          (System/setOut std-out)))))

  (testing "through"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topic-c (mock/topic "topic-c")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/through topic-b)
                                          (k/to! topic-c))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])

      (is (= [1 1]
             (mock/consume driver topic-b)))

      (is (= [1 1]
             (mock/consume driver topic-c)))))

  (testing "to!"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])

      (is (= [1 1]
             (mock/consume driver topic-b)))))

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
                                        (k/to! pos-stream topic-pos)
                                        (k/to! neg-stream topic-neg))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 -1])

      (is (= [1 1]
             (mock/consume driver topic-pos)))

      (is (= [1 -1]
             (mock/consume driver topic-neg)))))

  (testing "flat-map"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/flat-map (fn [[k v]]
                                                        [[k v]
                                                         [k 0]]))
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])

      (is (= [1 1]
             (mock/consume driver topic-b)))
      (is (= [1 0]
             (mock/consume driver topic-b)))))

  (testing "flat-map-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/flat-map-values (fn [v]
                                                               [v (inc v)]))
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])

      (is (= [1 1]
             (mock/consume driver topic-b)))
      (is (= [1 2]
             (mock/consume driver topic-b)))))

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
                                            (k/to! topic-c)))))
          produce-a (mock/producer driver topic-a)
          produce-b (mock/producer driver topic-b)]

      (produce-a [1 1] 1)
      (produce-b [1 2] 100)
      (produce-b [1 4] 10000) ;; Outside of join window
      (is (= [1 3] (mock/consume driver topic-c)))
      (is (not (mock/consume driver topic-c)))))

  (testing "map"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/map (fn [[k v]]
                                                   [(inc k) (inc v)]))
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])

      (is (= [2 2]
             (mock/consume driver topic-b)))))

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
                                            (k/to! topic-c)))))
          produce-a (mock/producer driver topic-a)
          produce-b (mock/producer driver topic-b)]

      (produce-a [1 1] 1)
      (produce-b [1 2] 100)
      (produce-b [1 4] 10000) ;; Outside of join window
      (is (= [1 1] (mock/consume driver topic-c)))
      (is (= [1 3] (mock/consume driver topic-c)))
      (is (= [1 4] (mock/consume driver topic-c)))
      (is (not (mock/consume driver topic-c)))))

  (testing "process!"
    (let [topic-a (mock/topic "topic-a")
          records (atom [])
          driver (mock/build-driver (fn [builder]
                                      (-> (k/kstream builder topic-a)
                                          (k/process! (fn [ctx k v]
                                                        (swap! records conj v))
                                                      []))))
          produce-a (mock/producer driver topic-a)]

      (produce-a [1 1] 1)
      (is (= [1] @records))))

  (testing "select-key"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/select-key (fn [[k v]]
                                                          (inc k)))
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])

      (is (= [2 1]
             (mock/consume driver topic-b)))))

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
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])
      (produce [1 4])

      (is (= [2 1] (mock/consume driver topic-b)))
      (is (= [2 3] (mock/consume driver topic-b)))
      (is (= [2 7] (mock/consume driver topic-b)))))

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
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])
      (produce [1 4])

      (is (= [1 1] (mock/consume driver topic-b)))
      (is (= [1 3] (mock/consume driver topic-b)))
      (is (= [1 7] (mock/consume driver topic-b)))
      (is (not (mock/consume driver topic-b))))))

(deftest KTable
  (testing "filter"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/filter (fn [[k v]]
                                                              (not (zero? v))))
                                                  (k/to-kstream)
                                                  (k/to! topic-b))))]
        (let [produce (mock/producer driver topic-a)]

          (produce [1 2])
          (produce [1 0])

          (is (= [1 2]
                 (mock/consume driver topic-b)))
          (is (= [1 nil] (mock/consume driver topic-b))) ;; Tombstone from filter
          (is (not (mock/consume driver topic-b)))))))

  (testing "filter-not"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/filter-not (fn [[k v]]
                                                                  (not (zero? v))))
                                                  (k/to-kstream)
                                                  (k/to! topic-b))))]
        (let [produce (mock/producer driver topic-a)]

          (produce [1 0])
          (produce [1 2])

          (is (= [1 0]
                 (mock/consume driver topic-b)))
          (is (= [1 nil] (mock/consume driver topic-b))) ;; Tombstone from filter
          (is (not (mock/consume driver topic-b)))))))

  (testing "map-values"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (-> (k/ktable builder topic-a)
                                                  (k/map-values (fn [v]
                                                                  (inc v)))
                                                  (k/to-kstream)
                                                  (k/to! topic-b))))]
        (let [produce (mock/producer driver topic-a)]

          (produce [1 0])
          (produce [1 2])
          (produce [2 0])

          (is (= [1 1]
                 (mock/consume driver topic-b)))
          (is (= [1 3]
                 (mock/consume driver topic-b)))
          (is (= [2 1]
                 (mock/consume driver topic-b)))
          (is (not (mock/consume driver topic-b)))))))

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
                                                  (k/to! topic-c))))]
        (let [produce (mock/producer driver topic-a)]

          (produce [1 0])
          (produce [2 0])

          (is (= [2 1]
                 (mock/consume driver topic-c)))
          (is (= [2 2]
                 (mock/consume driver topic-c)))
          (is (not (mock/consume driver topic-c)))))))

  (testing "join"
    (let [topic-a (mock/topic "table-a")
          topic-b (mock/topic "table-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/ktable builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/join left right +)
                                                    (k/to-kstream)
                                                    (k/to! topic-c)))))]
        (let [produce-left (mock/producer driver topic-a)
              produce-right (mock/producer driver topic-b)]

          (produce-left [1 1])
          (produce-right [1 2])
          (produce-left [1 4])
          (produce-left [2 42])

          (is (= [1 3]
                 (mock/consume driver topic-c)))
          (is (= [1 6]
                 (mock/consume driver topic-c)))
          (is (not
                 (mock/consume driver topic-c)))))))

  (testing "outer-join"
    (let [topic-a (mock/topic "table-a")
          topic-b (mock/topic "table-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/ktable builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/outer-join left right safe-add)
                                                    (k/to-kstream)
                                                    (k/to! topic-c)))))]
        (let [produce-left (mock/producer driver topic-a)
              produce-right (mock/producer driver topic-b)]

          (produce-left [1 1])
          (produce-right [1 2])
          (produce-left [1 4])
          (produce-left [2 42])

          (is (= [1 1]
                 (mock/consume driver topic-c)))
          (is (= [1 3]
                 (mock/consume driver topic-c)))
          (is (= [1 6]
                 (mock/consume driver topic-c)))
          (is (= [2 42]
                 (mock/consume driver topic-c)))
          (is (not
                 (mock/consume driver topic-c)))))))

  (testing "left-join"
    (let [topic-a (mock/topic "table-a")
          topic-b (mock/topic "table-b")
          topic-c (mock/topic "topic-c")]

      (with-open [driver (mock/build-driver (fn [builder]
                                              (let [left (k/ktable builder topic-a)
                                                    right (k/ktable builder topic-b)]
                                                (-> (k/left-join left right safe-add)
                                                    (k/to-kstream)
                                                    (k/to! topic-c)))))]
        (let [produce-left (mock/producer driver topic-a)
              produce-right (mock/producer driver topic-b)]

          (produce-left [1 1])
          (produce-right [1 2])
          (produce-left [1 4])
          (produce-right [2 42])

          (is (= [1 1]
                 (mock/consume driver topic-c)))
          (is (= [1 3]
                 (mock/consume driver topic-c)))
          (is (= [1 6]
                 (mock/consume driver topic-c)))
          (is (not
                 (mock/consume driver topic-c))))))))

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
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])

      (is (= [1 1]
             (mock/consume driver topic-b)))
      (is (= [1 2]
             (mock/consume driver topic-b)))
      (is (not (mock/consume driver topic-b)))))

  (testing "reduce"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          driver (mock/build-driver (fn [builder]
                                      (-> builder
                                          (k/kstream topic-a)
                                          (k/group-by (fn [[k v]] (long (/ k 10))) topic-a)
                                          (k/reduce + topic-a)
                                          (k/to-kstream)
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])
      (produce [10 2])

      (is (= [0 1]
             (mock/consume driver topic-b)))
      (is (= [0 3]
             (mock/consume driver topic-b)))
      (is (= [1 2]
             (mock/consume driver topic-b)))
      (is (not (mock/consume driver topic-b)))))

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
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1])
      (produce [1 2])
      (produce [10 2])

      (is (= [0 -9]
             (mock/consume driver topic-b)))
      (is (= [0 -7]
             (mock/consume driver topic-b)))
      (is (= [1 -8]
             (mock/consume driver topic-b)))
      (is (not (mock/consume driver topic-b)))))

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
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1] 1000)
      (produce [1 2] 1500)
      (produce [1 4] 5000)

      (is (= [0 1]
             (mock/consume driver topic-b)))
      (is (= [0 3]
             (mock/consume driver topic-b)))
      (is (= [0 4]
             (mock/consume driver topic-b)))
      (is (not (mock/consume driver topic-b)))))

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
                                          (k/to! topic-b))))
          produce (mock/producer driver topic-a)]

      (produce [1 1] 1000)
      (produce [1 2] 1500)
      (produce [1 3] 5000)
      (produce [12 3] 5100)

      (is (= [0 1]
             (mock/consume driver topic-b)))
      (is (= [0 nil]
             (mock/consume driver topic-b)))
      (is (= [0 3]
             (mock/consume driver topic-b)))
      (is (= [0 3]
             (mock/consume driver topic-b)))
      (is (= [1 3]
             (mock/consume driver topic-b)))
      (is (not (mock/consume driver topic-b))))))

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
                                                  (k/to! topic-b))))]
        (let [produce (mock/producer driver topic-a)]

          (produce [1 1])
          (produce [2 2])
          (produce [2 nil])
          (produce [10 3])

          (is (= [0 1]
                 (mock/consume driver topic-b)))
          (is (= [0 3]
                 (mock/consume driver topic-b)))
          (is (= [0 1]
                 (mock/consume driver topic-b)))
          (is (= [1 3]
                 (mock/consume driver topic-b)))
          (is (not (mock/consume driver topic-b)))))))

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
                                                  (k/to! topic-c))))]
        (let [produce (mock/producer driver topic-a)]

          (produce [1 0])
          (produce [2 0])

          (is (= [2 1]
                 (mock/consume driver topic-c)))
          (is (= [2 2]
                 (mock/consume driver topic-c)))
          (is (not (mock/consume driver topic-c)))))))

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
                                                  (k/to! topic-b))))]
        (let [produce (mock/producer driver topic-a)]

          (produce [1 1])
          (produce [2 2])
          (produce [2 nil])
          (produce [10 3])

          (is (= [0 1]
                 (mock/consume driver topic-b)))
          (is (= [0 3]
                 (mock/consume driver topic-b)))
          (is (= [0 1]
                 (mock/consume driver topic-b)))
          (is (= [1 3]
                 (mock/consume driver topic-b)))
          (is (not (mock/consume driver topic-b))))))))

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
                                                    (k/to! topic-c)))))]
        (let [produce-stream (mock/producer driver topic-a)
              produce-table (mock/producer driver topic-b)]

          (produce-stream [1 1])
          (produce-table [1 2])
          (produce-stream [1 4])

          (is (= [1 6]
                 (mock/consume driver topic-c)))
          (is (not (mock/consume driver topic-c)))))))

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
                                                    (k/to! topic-c)))))]
        (let [produce-stream (mock/producer driver topic-a)
              produce-table (mock/producer driver topic-b)]

          (produce-stream [1 1])
          (produce-table [1 2])
          (produce-stream [1 4])

          (is (= [1 1]
                 (mock/consume driver topic-c)))
          (is (= [1 6]
                 (mock/consume driver topic-c)))
          (is (not (mock/consume driver topic-c))))))))
