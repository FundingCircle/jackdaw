(ns jackdaw.test-test
  (:require
   [clojure.test :refer :all]
   [jackdaw.serdes.avro.schema-registry :as reg]
   [jackdaw.streams :as k]
   [jackdaw.test :as jd.test]
   [jackdaw.test-config :refer [test-config]]
   [jackdaw.test.commands :as cmd]
   [jackdaw.test.fixtures :as fix]
   [jackdaw.test.serde :as serde]
   [jackdaw.test.transports :as trns]
   [jackdaw.test.transports.kafka]
   [jackdaw.test.transports.mock]
   [jackdaw.test.middleware :refer [with-status]])
  (:import
   (java.util Properties)
   (org.apache.kafka.streams TopologyTestDriver)))

(def foo-topic
  (serde/resolver {:topic-name "foo"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :string
                   :value-serde :json}))

(def test-in
  (serde/resolver {:topic-name "test-in"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :string
                   :value-serde :json}))

(def test-out
  (serde/resolver {:topic-name "test-out"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :string
                   :value-serde :json}))

(def kafka-config {"bootstrap.servers" (format "%s:%s"
                                               (get-in (test-config) [:broker :host])
                                               (get-in (test-config) [:broker :port]))
                   "group.id" "kafka-write-test"})

(defn kafka-transport
  []
  (trns/transport {:type :kafka
                   :config kafka-config
                   :topics {"foo" foo-topic}}))

(def record-meta-fields [:topic-name
                         :offset
                         :partition
                         :serialized-key-size
                         :serialized-value-size])

(defn by-id
  [topic id]
  (fn [journal]
    (->> (get-in journal [:topics topic])
         (filter (fn [m]
                   (= id (get-in m [:value :id]))))
         first)))

(deftest test-run-test
  (testing "the run test machinery"
    (let [m {:executor (-> (fn [m c]
                             (let [[cmd & params] c]
                               (apply ({:min (fn [v] {:result (apply min v)})
                                        :max (fn [v] {:result (apply max v)})
                                        :is-1 (fn [v] (if (= v 1)
                                                        {:result true}
                                                        {:error :not-1}))}
                                       cmd)
                                      params)))
                           with-status)
             :journal (atom {})}]
      (testing "works properly"
        (let [{:keys [results journal]}
              (jd.test/run-test m [[:min [1 2 3]]
                                   [:max [1 2 3]]
                                   [:is-1 1]])]
          (is (= 3 (count results)))
          (is (every? #(= :ok %) (map :status results)))))

      (testing "execution stops on an error"
        (let [{:keys [results journal]}
              (jd.test/run-test m [[:min [1 2 3]]
                                   [:is-1 2]
                                   [:max [1 2 3]]])]
          (is (= 2 (count results)))
          (is (= :ok (:status (first results))))
          (is (= :error (:status (second results))))))

      (testing "execution stops on an unknown command"
        (let [{:keys [results journal]}
              (jd.test/run-test m [[:min [1 2 3]]
                                   [:foo 2]
                                   [:max [1 2 3]]])]
          (is (= 2 (count results)))
          (is (= :ok (:status (first results))))
          (is (= :error (:status (second results)))))))))

(deftest test-write-then-watch
  (testing "write then watch"
    (fix/with-fixtures [(fix/topic-fixture kafka-config {"foo" foo-topic})]
      (with-open [t (jd.test/test-machine (kafka-transport))]
        (let [write [:write! "foo" {:id "msg1" :payload "yolo"}]
              watch [:watch (by-id "foo" "msg1")
                     {:info "failed to find foo with id=msg1"}]

              {:keys [results journal]} (jd.test/run-test t [write watch])
              [write-result watch-result] results]

          (testing "write result"
            (is (= :ok (:status write-result)))

            (doseq [record-meta record-meta-fields]
              (is (contains? write-result record-meta))))

          (testing "watch result"
            (is (= :ok (:status watch-result))))

          (testing "written records are journalled"
            (is (= {:id "msg1" :payload "yolo"}
                   (-> ((by-id "foo" "msg1") journal)
                       :value)))))))))

(deftest test-reuse-machine
  (fix/with-fixtures [(fix/topic-fixture kafka-config {"foo" foo-topic})]
    (with-open [t (jd.test/test-machine (kafka-transport))]
      (let [prog1 [[:write! "foo" {:id "msg2" :payload "yolo"}]
                   [:watch (by-id "foo" "msg2")
                    {:info "failed to find foo with id=msg2"}]]

            prog2 [[:write! "foo" {:id "msg3" :payload "you only live twice"}]
                   [:watch (by-id "foo" "msg3")
                    {:info "failed to find foo with id=msg3"}]]]

        (testing "run test sequence and inspect results"
          (let [{:keys [results journal]} (jd.test/run-test t prog1)]
            (is (every? #(= :ok (:status %)) results))
            (is (= {:id "msg2" :payload "yolo"}
                   (-> ((by-id "foo" "msg2") journal)
                       :value)))))

        (testing "run another test sequence and inspect results"
          (let [{:keys [results journal]} (jd.test/run-test t prog2)]
            (is (every? #(= :ok (:status %)) results))

            (testing "old results remain in the journal"
              (is (= {:id "msg2" :payload "yolo"}
                     (-> ((by-id "foo" "msg2") journal)
                         :value))))

            (testing "and new results have been added"
              (is (= {:id "msg3" :payload "you only live twice"}
                     (-> ((by-id "foo" "msg3") journal)
                         :value))))))))))
