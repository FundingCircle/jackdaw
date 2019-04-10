(ns jackdaw.test
  "A test-machine executes sequences of test commands

   Test machines can be constructed to operate against a variety of targets. For
   example:

     - a local development kafka cluster
     - a mock topology processor
     - a cluster shared with other users

   In each of these cases, as a test-author we typically want to do the same type
   of thing. Inject a bunch of events into the system, wait until the system under
   test has finished processing, see what comes out the other end, and check that
   looks good.

   But the mechanism by which data is injected and observed is different in each
   case. The test-machine exists so that the author doesn't care. The exact same
   test (or scenario) can be executed against a mock topology processor, a kafka
   cluster running on localhost, or (via smokin or the rest-proxy) a remote
   kafka cluster shared with other users."
  (:require
   [clojure.tools.logging :as log]
   [jackdaw.test.commands :refer [with-handler command-handler]]
   [jackdaw.test.commands.base]
   [jackdaw.test.commands.write]
   [jackdaw.test.commands.watch]
   [jackdaw.test.transports :as trns :refer [with-transport]]
   [jackdaw.test.transports.identity]
   [jackdaw.test.transports.kafka]
   [jackdaw.test.transports.mock]
   [jackdaw.test.transports.rest-proxy]
   [jackdaw.test.journal :refer [with-journal]]
   [jackdaw.test.middleware :refer [with-timing with-status with-journal-snapshots]]))

;; Information about the internal architecture. Extract this out to a wiki or
;; something.
;;
;; The `test-machine` function returns a map with the followng keys

;;   :executor    ;; (fn [machine cmd]
;;                ;;   )

;;   :exit-hooks  ;; []
;;   :consumer    {:messages (s/stream)} source of test output
;;   :producer    {:messages (s/stream)} sink for test-input

;; The consumer and producer keys are themselves maps that each contains a
;; `:messages` channel used for kafka IO. `(get-in machine [:producer :messages])`
;; is a channel into which the test-machine feeds test-input from the scenario and
;; `(get-in machine [:consumer :messages])` is a channel into which topic data is
;; read from kafka.

;; The `journaller` is used to asynchronously read topics of interest and log
;; their output in a `:journal` key. Test middlware may use the journal to obtain a
;; snapshot of the test input/output during the execution of a command. See the
;; middleware and the `watch` command for example usage
;; of this feature.

;; The `:exit-hooks` key may be used by middleware that requires the disposal of any
;; resources when the test machine is closed.

(def +default-executor+ (-> (fn [machine cmd]
                              ((:command-handler machine) machine cmd))
                            with-status
                            with-timing
                            with-journal-snapshots))

(defrecord TestMachine [executor
                        exit-hooks
                        consumer
                        producer]
  java.io.Closeable
  (close [this]
    (doseq [hook exit-hooks]
      (hook))
    (log/info "destroyed test machine")))

(defn test-machine
  "Returns a test-machine for use in conjunction with `run-test`.

   The test-machine is just a map to which we attach various stateful
   objects required during a typical test run involving an application
   which reads/writes to kafka.

   The first parameter is a `transport` which can be obtained by one of
   the transport returning functions defined below. The transport
   determines how exactly the test events will be injected into the system
   under test.

   The (optional) second parameter is an executor and if none is specified
   the `+default-executor+` is used."
  ([transport]
   (test-machine transport +default-executor+))

  ([transport executor]
   (when (empty? transport)
     (throw (ex-info "transport should provide producer and consumer keys"
                     {})))
   (let [m (-> {:executor executor
                :exit-hooks []}
               (with-transport transport)
               (with-journal (agent {:topics {}}))
               (with-handler command-handler)
               (map->TestMachine))]
     (log/info "created test machine")
     m)))

(defn run-test
  "Runs a sequence of test commands against a test-machine and returns a
   response map.

   The response map includes

     `:results` A sequence of execution results. One for each command
                attempted

     `:journal` A snapshot of all kafka output read by the test consumer

   The first parameter is a test-machine and the second is a list of
   commands to execute. Remember to use `with-open` on the test-machine
   to ensure that all resources are correcly torn down."
  [machine commands]
  (let [exe (fn [cmd]
              (try
                ((:executor machine) machine cmd)
                (catch Exception e
                  (log/error e "Uncaught exception while executing test command")
                  {:status :error
                   :error e})))]
    ;; run commands, stopping if one fails.
    {:results (loop [results []
                     cmd-list commands]
                (cond
                  (first cmd-list) (let [r (exe (first cmd-list))]
                                     (if (or (contains? r :error) (empty? (rest cmd-list)))
                                       (conj results r)
                                       (recur (conj results r) (rest cmd-list))))
                  :else results))
     :journal @(:journal machine)}))

(defn identity-transport
  "The identity transport simply injects input events directly into
   the journal.

   Like all transports, this has consumer and producer keys containing
   processes that pick up test-commands added by `run-test`. However, this
   transport simply echo's any write commands to the journal rather than
   performing the write against another system.

   Primarily used internally for test purposes but may also be useful
   for testing your watch functions. The parameters are ignored and
   exist only for compatibility with other transports."
  [_config _topics]
  (trns/transport {:type :identity}))

(defn kafka-transport
  "The kafka transport injects input events by sending them via a
   KafkaProducer with direct access to the kafka cluster. Likewise it
   gathers output by polling a KafkaConsumer subscribed to the listed
   topics and adding the results to the journal.

   The `config` is shared by the consumer and producer processes and
   used to construct the underlying KafkaProducer and KafkaConsumer
   respectively

   The `topics` parameter represents the \"topics of interest\" for
   this transport. It should be a {\"string\" topic-metadata} map
   which tells the producer how to serialize any write commands and
   tells the consumer how to deserialize output messages before
   adding them to the journal."
  [config topics]
  (trns/transport {:type :kafka
                   :config config
                   :topics topics}))

(defn mock-transport
  "The mock transport injects input events by submitting them to a
   TopologyTestDriver. Likewise it gathers output by polling the
   test driver's `.readOutput` method.

   The `topics` parameter has the same semantics as in the
   kafka-transport"
  [config topics]
  (trns/transport {:type :mock
                   :driver (get config :driver)
                   :topics topics}))

(defn rest-proxy-transport
  "The rest-proxy transport injects input events by submitting them
   to a remote kafka cluster via the confluent REST API. Likewise it
   gathers output by polling the /records endpoint of the confluent
   REST API.

   If avro topics are involved, the topics must also be configured
   with access to the confluent schema registry. It should be noted
   that both the schema registry and the rest-proxy services used
   in this scenario must be backed by the same kafka cluster (or
   at least mirror images of one another).

   The `topics` paramter has the same semantics as in the kafka-transport"
  [config topics]
  (trns/transport {:type :confluent-rest-proxy
                   :config config
                   :topics topics}))

(defn with-test-machine
  "Convenience wrapper for the test-machine.

   Creates a test-machine using the supplied `transport` and then
   passes it to the supplied `f`. Typical usage would look something
   like this:

   ```
   (with-test-machine (test-transport)
     (fn [machine]
       (let [{:keys [results journal]} (run-test machine test-commands)]
         (is (every? ok? results))
         (is (= (expected-topic-output test-commands)
                (actual-topic-output journal))))))
   ```"
  [transport f]
  (with-open [machine (test-machine transport)]
    (f machine)))
