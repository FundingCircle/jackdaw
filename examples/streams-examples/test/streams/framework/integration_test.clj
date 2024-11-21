(ns streams.framework.integration-test
  (:require
   [clj-uuid :as uuid]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :as pp]
   [clojure.test :as c.t]
   [jackdaw.test :as jd.test]
   [jackdaw.test.commands.watch :as watch]
   [jackdaw.test.journal :as j]
   [streams.core :as core]
   [streams.framework.admin-tools :as admin])
  (:import
   (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)
   (java.io File)
   (java.time LocalDateTime)
   (org.apache.kafka.streams Topology TopologyTestDriver)))

;; Journal Helpers

(defn msg-for-key [journal topic k]
  (filter (fn [m]
            (= (:key m) k)) (j/raw-messages journal topic)))

(defn watch-msg-count [topic c]
  (fn [journal]
    (let [ms (j/messages journal topic)]
      (= c (count ms)))))

(defn watch-msg-count-for-key [topic k c]
  (fn [journal]
    (let [ms (msg-for-key journal topic k)]
      (= c (count ms)))))

(defn result-ok? [tm-result]
  (doseq [err (remove (fn [r]
                        (= :ok (:status r))) tm-result)]
    (pp/pprint err))
  (every? #(= :ok %) (map :status tm-result)))

(defn export-journal
  ([journal] (export-journal journal "./test-results"))
  ([journal ^String directory]
   (let [rdir (File. directory)
         rfile (File. rdir (str "journal-" (LocalDateTime/now)))]
     (when (or (.exists rdir) (.mkdir rdir))
       (println (str "writing results to \"" rfile "\""))
       (println)
       (with-open [f (io/writer rfile)]
         (pp/pprint journal f))))))

(defn slurp-journal [file]
  (edn/read-string (slurp file)))

(defn summarise [journal]
  (pp/print-table
   (sort-by :topic
            (map (fn [[k v]]
                   {:topic k :messages (count v)}) (:topics journal)))))

(defn summarise-and-export [journal]
  (summarise journal)
  (export-journal journal))

;; Fixtures

(defn topic-fixture
  "Returns a fixture function that creates all the topics named in the app's topics
  config before running a test function."
  [config]
  (fn [t]
    (admin/create-topics config)
    (t)))

(defn reset-application-fixture
  "Returns a fixture that runs the kafka.tools.StreamsResetter with the supplied
  `reset-args` as parameters"
  [config]
  (fn [t]
    (admin/reset-application config)
    (t)))

(def ^:dynamic *test-machine-transport-fn* nil)

(defn- kafka-transport [{:keys [topics streams-settings] :as config}]
  (jd.test/kafka-transport
   {"bootstrap.servers" (:bootstrap-servers streams-settings)
    "group.id" (str "test-machine-consumer-" (uuid/v4))}
   topics))

(defn run-topology-fixture
  "Returns a fixture which runs the passed topology as a kafka stream, and binds
  a remote broker test machine transport to *test-machine-transport-fn*"
  [stream-build-fn config]
  (fn [t]
    (let [stream (core/run-topology stream-build-fn (assoc config :hard-exit-on-error false))]
      (try
        (binding [*test-machine-transport-fn* (fn []
                                                (kafka-transport config))]
          (t))
        (finally
          (.close stream))))))

(defn- mock-transport [^Topology topology
                      {:keys [topics streams-settings] :as config}]
  (jd.test/mock-transport
   {:driver (TopologyTestDriver. topology
                                 (core/props-for streams-settings))}
   topics))

(defn mock-topology-fixture
  "Returns a fixture which creates a Topology Test Driver and binds a test machine
  transport using the driver to *test-machine-transport-fn*"
  [stream-build-fn config]
  (fn [t]
    (let [topology (core/build-topology stream-build-fn config)]
      (binding [*test-machine-transport-fn* (fn [] (mock-transport topology config))
                watch/*default-watch-timeout* 5000]
        (t)))))

;; mode of test
;; :mock       - run with TopologyTestDriver (requires no running kafka)
;; :integrated - creates required topics and starts topology, runs tests
;;               against kafka. Requires externally runnign kafka platform.
;; :remote     - assumes the app is already running, and just runs the tests
;;               against kafka.
(def test-machine-mode (atom :mock))

;; All-in-one integration test fixture

(defn integration-fixture
  "Runs a topology for a test. Will run in one of two modes based on
  `test-machine-mode`. It will set up the config passed in accordingly.
  Best used as a :once fixture.

  :mock
  Runs the topology using the TopologyTestDriver

  :integrated
  Runs the topology using the core streams running code, i.e. as an
  actual independent KafkaStreams instance. For this, a separately
  running instance of the confluent platform is required (broker, schema-reg)
  This mode will create any missing topics for the test
  and reset the topology it it already exists. The confluent platform
  must be started separately somehow (the fixture does not start it).

  :remote
  Assumes the topology under test is already running somehow else, and
  only sets up the test machine for writign to and observing Kafka. Not
  strictly useful for tests - but would allow a CI integration test to
  then also be run against a remote UAT env etc."
  [topology-build-fn config]
  (c.t/join-fixtures
   (condp = @test-machine-mode
     :mock
     ;; Resolve serdes with MockSchemaRegistryClient
     ;; Set the backing state stores location to a unique place
     (let [config-for-test (-> config
                               (core/reify-serdes-config (MockSchemaRegistryClient.))
                               (update-in [:streams-settings :state-dir]
                                          (fn [dir]
                                            (str dir "/" (uuid/v4)))))]
       [(mock-topology-fixture topology-build-fn config-for-test)])

     :integrated
     ;; Resolve serdes with real SchemaRegistryClient
     (let [config-for-test (core/reify-serdes-config config)]
       [(topic-fixture config-for-test)
        (reset-application-fixture config-for-test)
        (run-topology-fixture topology-build-fn config-for-test)])

     :remote
     ;; Resolve serdes with real SchemaRegistryClient, and bind a remote kafka
     ;; transport with a fixture
     (let [config-for-test (core/reify-serdes-config config)]
       [(fn [t]
          (binding [*test-machine-transport-fn* (fn [] (kafka-transport config-for-test))]
            (t)))])

     ;; unknown mode
     (throw (ex-info (str "Unknown integration mode " @test-machine-mode)
                     {:mode @test-machine-mode})))))

;; Running Tests

(defn run-test
  "Runs a test machine test. Must be called wrapped with one of the fixtures
  which binds *test-machine-transport-fn*, either:
  - mock-topology-fixture
  - run-topology-fixture
  The `integration-fixture` will call one of those also, based on the value
  of *test-machine-mode*"
  ([commands] (run-test commands c.t/*testing-contexts* ))
  ([commands descriptions]
   (when (not *test-machine-transport-fn*)
     (throw (ex-info "run-test must be called wrapped by a fixture which binds *test-machine-transport-fn*, see docs" {})))
   (doseq [m descriptions] (println "*" m))
   (jd.test/with-test-machine
     (*test-machine-transport-fn*)
     (fn [machine]
       (let [result (jd.test/run-test machine commands)]
         (summarise-and-export (:journal result))
         result)))))
