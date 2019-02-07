(ns jackdaw.test.fixtures-test
  (:require
   [clojure.test :refer :all]
   [jackdaw.test.fixtures :refer :all]
   [jackdaw.test-config :refer [test-config]])
  (:import
   (org.apache.kafka.clients.admin AdminClient NewTopic)))

(def topic-foo
  {:topic-name "foo"
   :partition-count 1
   :replication-factor 1
   :config {}})

(def kafka-config
  {"bootstrap.servers" (format "%s:%s"
                               (get-in (test-config) [:broker :host])
                               (get-in (test-config) [:broker :port]))})

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
                                 
