(ns jackdaw.test.fixtures-test
  (:require
   [clojure.test :refer :all]
   [jackdaw.test.fixtures :refer :all])
  (:import
   (org.apache.kafka.clients.admin AdminClient)))

(set! *warn-on-reflection* false)

(def topic-foo
  {:topic-name "foo"
   :partition-count 1
   :replication-factor 1
   :config {}})

(def kafka-config
  {"bootstrap.servers" "localhost:9092"})

(def test-topics
  (let [topics {"foo" topic-foo}]
    (topic-fixture kafka-config topics)))

(defn- topic-exists?
  [client t]
  (contains? (-> (list-topics client)
                 (.names)
                 (deref)
                 (set))
             (:topic-name t)))

(deftest test-topic-fixture
  (with-fixtures [(topic-fixture kafka-config {"foo" topic-foo})]
    (with-open [client (AdminClient/create kafka-config)]
      (is (topic-exists? client topic-foo)))))

(defn test-resetter
  ""
  {:style/indent 1}
  [{:keys [app-config reset-params reset-fn]} assertion-fn]
  (let [reset-args (atom [])
        error-data (atom {})
        test-fn (fn []
                  (is true "fake test function"))
        fix-fn (reset-application-fixture app-config reset-params
                                          (partial reset-fn reset-args))]

    ;; invoke the reset-application fixture with the sample test-fn
    (try
      (fix-fn test-fn)
      (catch Exception e
        (reset! error-data (ex-data e))))

    (assertion-fn {:resetter (first @reset-args)
                   :reset-args (second @reset-args)
                   :error-data @error-data})))

(deftest test-reset-application-fixture
  (test-resetter {:app-config {"application.id" "yolo"
                               "bootstrap.servers" "kafka.test:9092"}
                  :reset-params ["--foo" "foo"
                                 "--bar" "bar"]
                  :reset-fn (fn [reset-args rt args]
                              (reset! reset-args [rt args])
                              0)}
    (fn [{:keys [resetter reset-args error-data]}]
      (is (instance? kafka.tools.StreamsResetter resetter))
      (is (= ["--application-id" "yolo"
              "--bootstrap-servers" "kafka.test:9092"
              "--foo" "foo"
              "--bar" "bar"]
             reset-args))
      (is (empty? error-data)))))

(deftest test-reset-application-fixture-failure
  (test-resetter {:app-config {"application.id" "yolo"
                               "bootstrap.servers" "kafka.test:9092"}
                  :reset-params ["--foo" "foo"
                                 "--bar" "bar"]
                  :reset-fn (fn [reset-args rt args]
                              (reset! reset-args [rt args])
                              (.write *err* "helpful error message\n")
                              (.write *out* "essential application info\n")
                              1)}
    (fn [{:keys [resetter reset-args error-data]}]
      (is (instance? kafka.tools.StreamsResetter resetter))
      (is (= 1 (:status error-data)))
      (is (= "helpful error message\n" (:err error-data)))
      (is (= "essential application info\n" (:out error-data))))))
