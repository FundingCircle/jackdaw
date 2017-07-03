(ns jackdaw.streams-test
  "Tests of the kafka streams wrapper."
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [jackdaw.streams :as k :refer [IKStream IKTable ITopologyBuilder]]
            [jackdaw.streams.interop :as interop]
            [jackdaw.streams.mock :as mock]
            [jackdaw.streams.lambdas :as lambdas :refer [key-value]])
  (:import [org.apache.kafka.streams.kstream JoinWindows TimeWindows Transformer ValueTransformer]
           [org.apache.kafka.test MockProcessorSupplier MockProcessorSupplier$MockProcessor]))

(deftest TopologyBuilder
  (testing "merge"
    (let [topology-builder (mock/topology-builder)
          kstream-a (k/kstream topology-builder (mock/topic "topic-a"))
          kstream-b (k/kstream topology-builder (mock/topic "topic-b"))
          merged-kstream (k/merge topology-builder [kstream-a kstream-b])]
      (is (satisfies? IKStream merged-kstream))
      (is (= #{"topic-a" "topic-b"} (k/source-topics topology-builder)))))

  (testing "new-name"
    (let [new-name (k/new-name (mock/topology-builder) "foo")]
      (is (string/starts-with? new-name "foo"))))

  (testing "kstream"
    (let [topology-builder (mock/topology-builder)
          kstream-a (-> topology-builder
                        (k/kstream (mock/topic "topic-a")))]

      (is (satisfies? IKStream kstream-a))
      (is (= #{"topic-a"} (k/source-topics topology-builder)))))

  (testing "kstreams"
    (let [topology-builder (mock/topology-builder)
          kstream (-> topology-builder
                      (k/kstreams [(mock/topic "topic-a")
                                   (mock/topic "topic-b")]))]

      (is (satisfies? IKStream kstream))
      (is (= #{"topic-a" "topic-b"}
             (k/source-topics topology-builder)))))

  (testing "ktable"
    (let [topology-builder (mock/topology-builder)
          ktable-a (-> topology-builder
                       (k/ktable (mock/topic "topic-a")))]
      (is (satisfies? IKTable ktable-a))
      (is (= #{"topic-a"} (k/source-topics topology-builder)))))


  (testing "topology-builder*"
    (is (instance? org.apache.kafka.streams.processor.TopologyBuilder
                   (k/topology-builder* (mock/topology-builder)))))

  (testing "topology-builder"
    (is (satisfies? ITopologyBuilder (interop/topology-builder)))))

(deftest KStream
  (testing "left-join"
    (let [topology-builder (mock/topology-builder)
          left-topic (mock/topic "left-topic")
          right-topic (mock/topic "right-topic")
          left-kstream (k/kstream topology-builder left-topic)
          right-ktable (k/ktable topology-builder right-topic)
          topology (-> left-kstream
                       (k/left-join right-ktable (fn [v1 v2] [v1 v2]))
                       (mock/build))]

      (-> topology
          (mock/send right-topic 1 1)
          (mock/send left-topic 1 2))

      (let [result (mock/collect topology)]
        (is (= ["1:[2 1]"] result)))))

  (testing "left-join (with custom serdes)"
    (let [topology-builder (mock/topology-builder)
          left-topic (mock/topic "left-topic")
          right-topic (mock/topic "right-topic")
          left-kstream (k/kstream topology-builder left-topic)
          right-ktable (k/ktable topology-builder right-topic)
          topology (-> left-kstream
                       (k/left-join right-ktable (fn [v1 v2] [v1 v2]) left-topic)
                       (mock/build))]

      (-> topology
          (mock/send right-topic 1 1)
          (mock/send left-topic 1 2))

      (let [result (mock/collect topology)]
        (is (= ["1:[2 1]"] result)))))

  (testing "for-each!"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          kstream (k/kstream topology-builder topic-a)
          results (atom {})]
      (k/for-each! kstream (fn [[k v]] (swap! results assoc k v)))
      (let [topology (mock/build kstream)]
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (= {1 2} @results))))))

  (testing "filter"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/filter (fn [[k v]] (> v 1)))
                       (mock/build))]

      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 2 2))

      (let [result (mock/collect topology)]
        (is (= ["2:2"] result)))))

  (testing "filter-not"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/filter-not (fn [[k v]] (> v 1)))
                       (mock/build))]

      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 2 2))

      (let [result (mock/collect topology)]
        (is (= ["1:1"] result)))))

  (testing "map-values"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/map-values (fn [v] (str "new-" v)))
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:new-2"] result)))))

  (testing "print!"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          kstream (k/kstream topology-builder topic-a)
          std-out System/out
          mock-out (java.io.ByteArrayOutputStream.)]
      (try
        (System/setOut (java.io.PrintStream. mock-out))
        (k/print! kstream)

        (let [topology (mock/build kstream)]
          (mock/send topology topic-a 1 2)
          (let [result (mock/collect topology)]
            (is (= "[KSTREAM-SOURCE-0000000000]: 1 , 2\n" (.toString mock-out)))))
        (finally
          (System/setOut std-out)))))

  (testing "print! (with custom serdes)"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          kstream (k/kstream topology-builder topic-a)
          std-out System/out
          mock-out (java.io.ByteArrayOutputStream.)]
      (try
        (System/setOut (java.io.PrintStream. mock-out))
        (k/print! kstream topic-a)

        (let [topology (mock/build kstream)]
          (mock/send topology topic-a 1 2)
          (let [result (mock/collect topology)]
            (is (= "[KSTREAM-SOURCE-0000000000]: 1 , 2\n" (.toString mock-out)))))
        (finally
          (System/setOut std-out)))))

  (testing "through"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/through topic-b)
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:2"] result)))))

  (testing "through (with custom partitioner)"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          partitioner-fn (fn [k v c] (rand-int 0 c))
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/through partitioner-fn topic-b)
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:2"] result)))))

  (testing "to!"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          kstream (-> (mock/topology-builder)
                      (k/kstream topic-a))]
      (let [topology (mock/build kstream)]
        (k/to! kstream topic-b)
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (= ["1:2"] result))))))

  (testing "to! (with custom partitioner)"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          partitioner-fn (fn [k v c] (rand-int 0 c))
          kstream (-> (mock/topology-builder)
                      (k/kstream topic-a))
          topology (mock/build kstream)]
      (k/to! kstream partitioner-fn topic-b)
      (mock/send topology topic-a 1 2)
      (let [result (mock/collect topology)]
        (is (= ["1:2"] result)))))

  (testing "write-as-text!"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          kstream (k/kstream topology-builder topic-a)
          temp-file (java.io.File/createTempFile
                     "jackdaw.stream+write-as-text-test"
                     "txt")]
      (k/write-as-text! kstream (.getPath temp-file))
      (let [topology (mock/build kstream)]
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (=  "[KSTREAM-SOURCE-0000000000]: 1 , 2\n" (slurp temp-file)))))))

  (testing "write-as-text! (with custom serdes)"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          kstream (k/kstream topology-builder topic-a)
          temp-file (java.io.File/createTempFile
                     "jackdaw.stream+write-as-text-test"
                     "txt")]
      (k/write-as-text! kstream (.getPath temp-file) topic-a)
      (let [topology (mock/build kstream)]
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (= "[KSTREAM-SOURCE-0000000000]: 1 , 2\n" (slurp temp-file)))))))

  (testing "aggregate-by-key"
    (let [initializer-fn (constantly 0)
          aggregator-fn (fn [acc [k v]] (+ acc v))
          topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/aggregate-by-key initializer-fn aggregator-fn topic-a)
                       (k/to-kstream)
                       (mock/build))]
      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 1 2)
          (mock/send topic-a 2 3)
          (mock/send topic-a 2 4))
      (let [result (mock/collect topology)]
        (is (= ["1:1" "1:3" "2:3" "2:7"] result)))))

  (testing "aggregate-by-key-windowed"
    (let [initializer-fn (constantly 0)
          aggregator-fn (fn [acc [k v]] (+ acc v))
          topic-a (mock/topic "topic-a")
          windows (.. (TimeWindows/of 4)
                      (advanceBy 2))
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/aggregate-by-key-windowed initializer-fn
                                                    aggregator-fn
                                                    windows
                                                    topic-a)
                       (k/to-kstream)
                       (mock/build))]
      (-> topology
          (mock/send topic-a, 0, 0)
          (mock/send topic-a, 1, 1)
          (mock/send topic-a, 2, 2)
          (mock/send topic-a, 3, 3)
          (mock/send topic-a, 4, 4)
          (mock/send topic-a, 5, 5)
          (mock/send topic-a, 6, 6))
      (let [result (mock/collect topology)]
        (is (= ["[0@0]:0"
                "[1@0]:1"
                "[2@0]:2"
                "[2@2]:2"
                "[3@0]:3"
                "[3@2]:3"
                "[4@2]:4"
                "[4@4]:4"
                "[5@2]:5"
                "[5@4]:5"
                "[6@4]:6"
                "[6@6]:6"]
               result)))))

  (testing "branch"
    (let [topic-a (mock/topic "topic-a")
          predicate (fn [[k v]] (> v 1))
          [kstream-a kstream-b] (-> (mock/topology-builder)
                                    (k/kstream topic-a)
                                    (k/branch [predicate (complement predicate)]))]
      (let [topology-a (mock/build kstream-a)]
        (-> topology-a
            (mock/send topic-a 1 1)
            (mock/send topic-a 2 2))
        (let [result-a (mock/collect topology-a)]
          (is (= ["2:2"] result-a))))

      #_(let [topology-b (mock/build kstream-b)]
          (-> topology-b
              (mock/send topic-a 1 1)
              (mock/send topic-a 2 2))
          (let [result-b (mock/collect topology-b)]
            (is (= ["1:1"] result-b))))))

  (testing "count-by-key"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/count-by-key topic-a)
                       (k/to-kstream)
                       (mock/build))]
      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 1 2)
          (mock/send topic-a 2 3)
          (mock/send topic-a 2 4))
      (let [result (mock/collect topology)]
        (is (= ["1:1" "1:2" "2:1" "2:2"] result)))))

  (testing "count-by-key"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/count-by-key topic-a)
                       (k/to-kstream)
                       (mock/build))]
      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 1 2)
          (mock/send topic-a 2 3)
          (mock/send topic-a 2 4))
      (let [result (mock/collect topology)]
        (is (= ["1:1" "1:2" "2:1" "2:2"] result)))))

  (testing "count-by-key-windowed"
    (let [topic-a (mock/topic "topic-a")
          windows (.. (TimeWindows/of 4)
                      (advanceBy 2))
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/count-by-key-windowed windows topic-a)
                       (k/to-kstream)
                       (mock/build))]
      (-> topology
          (mock/send topic-a 0 0)
          (mock/send topic-a 1 1)
          (mock/send topic-a 2 2)
          (mock/send topic-a 3 3)
          (mock/send topic-a 4 4)
          (mock/send topic-a 5 5)
          (mock/send topic-a 6 6))
      (let [result (mock/collect topology)]
        (is (= ["[0@0]:1"
                "[1@0]:1"
                "[2@0]:1"
                "[2@2]:1"
                "[3@0]:1"
                "[3@2]:1"
                "[4@2]:1"
                "[4@4]:1"
                "[5@2]:1"
                "[5@4]:1"
                "[6@4]:1"
                "[6@6]:1"]
               result)))))

  (testing "flat-map"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/flat-map (fn [[k v]] [[k [v 1]] [k [v 2]]]))
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:[2 1]" "1:[2 2]"] result)))))

  (testing "flat-map-values"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/flat-map-values (fn [v] [[v 1] [v 2]]))
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:[2 1]" "1:[2 2]"] result)))))

  (testing "join-windowed"
    (let [topology-builder (mock/topology-builder)
          left-topic (mock/topic "left-topic")
          right-topic (mock/topic "right-topic")
          left-kstream (k/kstream topology-builder left-topic)
          right-kstream (k/kstream topology-builder right-topic)
          windows (JoinWindows/of 1000)
          topology (-> left-kstream
                       (k/join-windowed right-kstream
                                        (fn [v1 v2] [v1 v2])
                                        windows
                                        left-topic
                                        right-topic)
                       (mock/build))]

      (-> topology
          (mock/send left-topic 1 1)
          (mock/send left-topic 1 2)
          (mock/send left-topic 2 1)
          (mock/send left-topic 2 2))

      (let [result (mock/collect topology)]
        (is (= [] result)))))

  (testing "map"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/map (fn [[k v]] [v k]))
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["2:1"] result)))))

  (testing "outer-join-windowed"
    (let [topology-builder (mock/topology-builder)
          left-topic (mock/topic "left-topic")
          right-topic (mock/topic "right-topic")
          left-kstream (k/kstream topology-builder left-topic)
          right-kstream (k/kstream topology-builder right-topic)
          windows (JoinWindows/of 1000)
          topology (-> left-kstream
                       (k/outer-join-windowed right-kstream
                                              (fn [v1 v2] [v1 v2])
                                              windows
                                              left-topic
                                              right-topic)
                       (mock/build))]

      (-> topology
          (mock/send left-topic 1 1)
          (mock/send left-topic 1 2)
          (mock/send left-topic 2 1)
          (mock/send left-topic 2 2))

      (let [result (mock/collect topology)]
        (is (= ["1:[1 nil]" "1:[2 nil]" "2:[1 nil]" "2:[2 nil]"] result)))))

  (testing "process!"
    (let [external-log (atom [])
          processor-fn (fn [ctx k v]
                         (swap! external-log conj {:ctx ctx, :k k, :v v}))
          topic-a (mock/topic "topic-a")
          kstream (-> (mock/topology-builder)
                      (k/kstream topic-a))]

      (k/process! kstream processor-fn [])

      (let [topology (mock/build kstream)]

        (mock/send topology topic-a 1 1)

        (let [{:keys [ctx k v]} (first @external-log)]
          (is (instance? org.apache.kafka.test.MockProcessorContext ctx))
          (is (= k 1))
          (is (= v 1)))

        (let [result (mock/collect topology)]
          (is (= ["1:1"] result))))))

  (testing "reduce-by-key"
    (let [reducer-fn +
          topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/reduce-by-key reducer-fn topic-a)
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 1 2)
          (mock/send topic-a 2 3)
          (mock/send topic-a 2 4))

      (let [result (mock/collect topology)]
        (is (= ["1:1" "1:3" "2:3" "2:7"] result)))))

  (testing "reduce-by-key-windowed"
    (let [reducer-fn +
          windows (.. (TimeWindows/of 4)
                      (advanceBy 2))
          topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/reduce-by-key-windowed reducer-fn windows topic-a)
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send topic-a 0 0)
          (mock/send topic-a 1 1)
          (mock/send topic-a 2 2)
          (mock/send topic-a 3 3)
          (mock/send topic-a 4 4)
          (mock/send topic-a 5 5)
          (mock/send topic-a 6 6))

      (let [result (mock/collect topology)]
        (is (= ["[0@0]:0"
                "[1@0]:1"
                "[2@0]:2"
                "[2@2]:2"
                "[3@0]:3"
                "[3@2]:3"
                "[4@2]:4"
                "[4@4]:4"
                "[5@2]:5"
                "[5@4]:5"
                "[6@4]:6"
                "[6@6]:6"]
               result)))))

  (testing "select-key"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/select-key (fn [[k v]] (* 10 k)))
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["10:2"] result)))))

  (testing "transform"
    (let [transformer-supplier-fn #(let [total (atom 0)]
                                     (reify Transformer
                                       (init [_ _])
                                       (close [_])
                                       (punctuate [_ timestamp]
                                         (key-value [-1 (int timestamp)]))
                                       (transform [_ k v]
                                         (swap! total + v)
                                         (key-value [(* k 2) @total]))))
          topic-a (mock/topic "topic-a")
          kstream (-> (mock/topology-builder)
                      (k/kstream topic-a)
                      (k/transform transformer-supplier-fn))]

      (let [topology (mock/build kstream)]

        (doseq [k [1 10 100 1000]]
          (mock/send topology topic-a k (* 10 k)))

        (let [result (mock/collect topology)]
          (is (= ["2:10" "20:110" "200:1110" "2000:11110"] result)))

        ;; TODO
        ;; Expose KStreamTestDriver to test punctuate
        )))

  (testing "transform-values"
    (let [value-transformer-supplier-fn #(let [total (atom 0)]
                                           (reify ValueTransformer
                                             (init [_ _])
                                             (close [_])
                                             (punctuate [_ timestamp]
                                               (int timestamp))
                                             (transform [_ v]
                                               (swap! total + v)
                                               @total)))
          topic-a (mock/topic "topic-a")
          kstream (-> (mock/topology-builder)
                      (k/kstream topic-a)
                      (k/transform-values value-transformer-supplier-fn))]

      (let [topology (mock/build kstream)]

        (doseq [k [1 10 100 1000]]
          (mock/send topology topic-a k (* 10 k)))

        (let [result (mock/collect topology)]
          (is (= ["1:10" "10:110" "100:1110" "1000:11110"] result)))

        ;; TODO
        ;; Expose KStreamTestDriver to test punctuate
        ))
    ))

(deftest KTable
  (testing "left-join"
    (let [topology-builder (mock/topology-builder)
          left-topic (mock/topic "left-topic")
          right-topic (mock/topic "right-topic")
          left-ktable (k/ktable topology-builder left-topic)
          right-ktable (k/ktable topology-builder right-topic)
          topology (-> left-ktable
                       (k/left-join right-ktable (fn [v1 v2] [v1 v2]))
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send left-topic 1 2)
          (mock/send right-topic 1 1))

      (let [result (mock/collect topology)]
        (is (= ["1:[2 nil]" "1:[2 1]"] result)))))

  (testing "for-each!"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          ktable (k/ktable topology-builder topic-a)
          results (atom {})]
      (k/for-each! ktable (fn [[k v]] (swap! results assoc k v)))
      (let [topology (mock/build (k/to-kstream ktable))]
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (= {1 2} @results))))))

  (testing "filter"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/filter (fn [[k v]] (> v 1)))
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 2 2))

      (let [result (mock/collect topology)]
        (is (= ["1:null" "2:2"] result)))))

  (testing "filter-not"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/filter-not (fn [[k v]] (> v 1)))
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 2 2))

      (let [result (mock/collect topology)]
        (is (= ["1:1" "2:null"] result)))))

  (testing "map-values"
    (let [topic-a (mock/topic "topic-a")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/map-values (fn [v] (str "new-" v)))
                       (k/to-kstream)
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:new-2"] result)))))

  (testing "print!"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          ktable (k/ktable topology-builder topic-a)
          std-out System/out
          mock-out (java.io.ByteArrayOutputStream.)]
      (try
        (System/setOut (java.io.PrintStream. mock-out))
        (k/print! ktable)

        (let [topology (mock/build (k/to-kstream ktable))]
          (mock/send topology topic-a 1 2)
          (let [result (mock/collect topology)]
            (is (= "[KTABLE-SOURCE-0000000001]: 1 , (2<-null)\n" (.toString mock-out)))))
        (finally
          (System/setOut std-out)))))

  (testing "print! (with custom serdes)"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          ktable (k/ktable topology-builder topic-a)
          std-out System/out
          mock-out (java.io.ByteArrayOutputStream.)]
      (try
        (System/setOut (java.io.PrintStream. mock-out))
        (k/print! ktable topic-a)

        (let [topology (mock/build (k/to-kstream ktable))]
          (mock/send topology topic-a 1 2)
          (let [result (mock/collect topology)]
            (is (= "[KTABLE-SOURCE-0000000001]: 1 , (2<-null)\n" (.toString mock-out)))))
        (finally
          (System/setOut std-out)))))

  (testing "through"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/through topic-b)
                       (k/to-kstream)
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:2"] result)))))

  (testing "through (with custom partitioner)"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          partitioner-fn (fn [k v c] (rand-int 0 c))
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/through partitioner-fn topic-b)
                       (k/to-kstream)
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:2"] result)))))

  (testing "to!"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          ktable (-> (mock/topology-builder)
                     (k/ktable topic-a))]
      (let [topology (mock/build (k/to-kstream ktable))]
        (k/to! ktable topic-b)
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (= ["1:2"] result))))))

  (testing "to! (with custom partitioner)"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          partitioner-fn (fn [k v c] (rand-int 0 c))
          ktable (-> (mock/topology-builder)
                     (k/ktable topic-a))
          topology (mock/build (k/to-kstream ktable))]
      (k/to! ktable partitioner-fn topic-b)
      (mock/send topology topic-a 1 2)
      (let [result (mock/collect topology)]
        (is (= ["1:2"] result)))))

  (testing "write-as-text!"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          ktable (k/ktable topology-builder topic-a)
          temp-file (java.io.File/createTempFile
                     "jackdaw.stream+write-as-text-test"
                     "txt")]
      (k/write-as-text! ktable (.getPath temp-file))
      (let [topology (mock/build (k/to-kstream ktable))]
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (= "[KTABLE-SOURCE-0000000001]: 1 , (2<-null)\n" (slurp temp-file)))))))

  (testing "write-as-text! (with custom serdes)"
    (let [topic-a (mock/topic "topic-a")
          topology-builder (mock/topology-builder)
          ktable (k/ktable topology-builder topic-a)
          temp-file (java.io.File/createTempFile
                     "jackdaw.stream+write-as-text-test"
                     "txt")]
      (k/write-as-text! ktable (.getPath temp-file) topic-a)
      (let [topology (mock/build (k/to-kstream ktable))]
        (mock/send topology topic-a 1 2)
        (let [result (mock/collect topology)]
          (is (= "[KTABLE-SOURCE-0000000001]: 1 , (2<-null)\n" (slurp temp-file)))))))

  (testing "group-by"
    (let [topic-a (mock/topic "topic-a")
          count-a (mock/topic "count-a")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/group-by (fn [[k v]] [v k]) topic-a)
                       (k/count count-a)
                       (k/to-kstream)
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["2:1"] result)))))

  (testing "group-by-key"
    (let [topic-a (mock/topic "topic-a")
          count-a (mock/topic "count-a")
          topology (-> (mock/topology-builder)
                       (k/kstream topic-a)
                       (k/group-by-key count-a)
                       (k/count count-a)
                       (k/to-kstream)
                       (mock/build))]

      (mock/send topology topic-a 1 2)
      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["1:1" "1:2"] result)))))

  (testing "join"
    (let [topology-builder (mock/topology-builder)
          left-topic (mock/topic "left-topic")
          right-topic (mock/topic "right-topic")
          left-ktable (k/ktable topology-builder left-topic)
          right-ktable (k/ktable topology-builder right-topic)
          topology (-> left-ktable
                       (k/join right-ktable (fn [v1 v2] [v1 v2]))
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send left-topic 1 2)
          (mock/send right-topic 1 1))

      (let [result (mock/collect topology)]
        (is (= ["1:[2 1]"] result)))))

  (testing "outer-join"
    (let [topology-builder (mock/topology-builder)
          left-topic (mock/topic "left-topic")
          right-topic (mock/topic "right-topic")
          left-ktable (k/ktable topology-builder left-topic)
          right-ktable (k/ktable topology-builder right-topic)
          topology (-> left-ktable
                       (k/outer-join right-ktable (fn [v1 v2] [v1 v2]))
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send right-topic 1 1)
          (mock/send left-topic 1 2))

      (let [result (mock/collect topology)]
        (is (= ["1:[nil 1]" "1:[2 1]"] result))))))


(deftest GroupedTable
  (testing "aggregate"
    (let [initializer-fn (constantly 0)
          adder-fn (fn [acc [k v]] (+ acc 3 v))
          subtractor-fn (fn [acc [k v]] (- acc 2 v))
          topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/group-by (fn [[k v]] [k v]) topic-a)
                       (k/aggregate initializer-fn adder-fn subtractor-fn topic-b)
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 1 2)
          (mock/send topic-a 2 3))

      (let [result (mock/collect topology)]
        (is (= ["1:4" "1:6" "2:6"] result)))))

  (testing "count"
    (let [topic-a (mock/topic "topic-a")
          count-a (mock/topic "count-a")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/group-by (fn [[k v]] [v k]) topic-a)
                       (k/count count-a)
                       (k/to-kstream)
                       (mock/build))]

      (mock/send topology topic-a 1 2)

      (let [result (mock/collect topology)]
        (is (= ["2:1"] result)))))

  (testing "reduce"
    (let [adder-fn +
          subtractor-fn -
          topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")
          topology (-> (mock/topology-builder)
                       (k/ktable topic-a)
                       (k/group-by (fn [[k v]] [k v]) topic-a)
                       (k/reduce adder-fn subtractor-fn topic-b)
                       (k/to-kstream)
                       (mock/build))]

      (-> topology
          (mock/send topic-a 1 1)
          (mock/send topic-a 1 2))

      (let [result (mock/collect topology)]
        (is (= ["1:1" "1:2"] result))))))

(deftest GlobalKTableTest
  (testing "inner global join"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")

          topology-builder (mock/topology-builder)

          left (k/kstream topology-builder topic-a)
          right (k/global-ktable topology-builder topic-b "asdf")

          topology (-> left
                       (k/join-global right
                                      (fn [[k v]]
                                        k)
                                      +)
                       (mock/build))]

      (-> topology
          (mock/send topic-b 1 1)
          (mock/send topic-a 1 2)
          (mock/send topic-a 2 3))

      (let [result (mock/collect topology)]
        (is (= ["1:3"] result)))))

  (testing "left global join"
    (let [topic-a (mock/topic "topic-a")
          topic-b (mock/topic "topic-b")

          topology-builder (mock/topology-builder)

          left (k/kstream topology-builder topic-a)
          right (k/global-ktable topology-builder topic-b "asdf")

          topology (-> left
                       (k/left-join-global right
                                           (fn [[k v]]
                                             k)
                                           (fn [a b]
                                             (+ a (or b 0))))
                       (mock/build))]

      (-> topology
          (mock/send topic-b 1 1)
          (mock/send topic-a 1 2)
          (mock/send topic-a 2 3))

      (let [result (mock/collect topology)]
        (is (= ["1:3" "2:3"] result))))))

(deftest kafka-streams-test
  (is (instance? org.apache.kafka.streams.KafkaStreams
                 (k/kafka-streams (mock/topology-builder)
                                  {"bootstrap.servers" "localhost:2181"
                                   "application.id" ""}))))
