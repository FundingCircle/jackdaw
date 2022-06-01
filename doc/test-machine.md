# The Test Machine

## Rationale

The test machine is what we came up with in an attempt to make it easier to
write more reliable integration tests. By handling reads and writes in a background
thread and making them available in a Clojure ref, the test author is relieved
of the requirement to manually `send!` and `recv` messages from kafka. The initial
aim was to simply 'fire' events at the system and then observe the outputs that
are (or are not) collected by the reader. In general, the aims are to allow the
creation of blackbox integration tests which are:

 * Generative, to avoid manual creation and alteration of test cases (or we will
not write them, and will eventually abandon them)
 * Reliable, so we trust the results (or we will ignore them)
 * Fast, so we get results quickly (or we will not run them)

The test machine also aims to be agnostic to the system it is testing, other
than that it should get input/output primarily from Kafka.

### Construction

The examples below, demonstrate how to create a test machine for executing a
sequence of "commands". The "local-machine" will execute the commands against a
local kafka cluster with all services running on their default ports. The
"remote-machine" in contrast executes commands over HTTP using the configured
rest-proxy. This can be useful in scenarios where you don't have direct access
to these services in a shared environment like uat/staging.

```clojure
(ns my.app-test
  (:require
    [my.app :as app]
    [jackdaw.serdes :refer [string-serde edn-serde]]
    [jackdaw.serdes.json :as jsj]
    [jackdaw.test :refer [test-machine]]
    [jackdaw.test.transports :as trns]))

(def local-kafka-config
  {"bootstrap.servers" "localhost:9092"
   "group.id" "my-app"})

(def remote-kafka-config
  {:bootstrap-uri "my-real-rest-proxy-url"
   :group-id "my-app"})

(def topic-config
  {:foo {:topic-name "foo"
         :key-serde (string-serde)
         :value-serde (jsj/serde)
         :partition-count 1
         :replication-factor 1}
   :bar {:topic-name "foo"
         :key-serde (string-serde)
         :value-serde (edn-serde)
         :partition-count 1
         :replication-factor 1}})

(defn local-machine []
  (let [t (trns/transport {:type :kafka
                           :config local-kafka-config
                           :topics topic-config})]
    (test-machine t)))

(defn remote-machine []
  (let [t (trns/transport {:type :confluent-rest-proxy
                           :config remote-kafka-config
                           :topics topic-config})]
   (test-machine t)))
```

### Serialization/Deserialization

The `topic-config` referenced in the snippet above is a mapping from topic-ids to
serialization/deserialization configurations. This means that tests can read/write
using the same serializers/deserializers used by your applications. So for example, on
encountering a command like

```clojure
[:write! :foo {:id 1, :msg "hello"}]
```

the test-machine will lookup `:foo` in the topic-config to get the `:key-serde`
and `:value-serde` which it will then use when writing the message. Similarly, all topics
listed in the topic-config are read using the corresponding deserializer.

### Lifecycle

The test-machine implements the `Closeable` protocol so be sure to use it in
conjunction with `with-open` to ensure that associated resources are shut down
cleanly when you are finished with a machine.

### Test Commands

Each test-command is a vector with the first item being a keyword representing the
type of operation this command represents, and subsequent items being command
specific arguments. Currently the following commands are the supported.

```clojure
  :write!   [topic-id msg opts]  Writes a message to the topic (Opts supports :key-fn, :partition, :partition-fn, :key, :timeout)
  :watch    [f opts]             Blocks until `(f @journal)` returns truthy
  :stop     []                   Stops processing commands. All subsequent commands
                                 are ignored
  :sleep    [sleep-ms]           Sleeps for `sleep-ms` milliseconds
  :println  [args]               Prints the supplied args to stdout
  :pprint   [args]               Pretty prints the supplied args to stdout
  :do       [f]                  Execute arbitrary function. The function should take
                                 a single argument, and will be passed the journal
                                 state (content of the journal atom).
  :do!      [f]                  Execute arbitrary function. The function should take
                                 a single argument, and will be passed the journal
                                 atom itself. Allows monitoring of the joural or
                                 the like to be injected.
  :inspect  [f]                  A debugging command, again executes an arbitrary
                                 function of a single argument. This function will
                                 be passed the entire test machine state.
```
For more details see the functions in the [jackdaw.test.commands](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT/api/jackdaw.test.commands) namespace.

### Test Results

A `test-machine` is used in conjunction with `run-test`, which runs a sequence of test commands against the test-machine. The first parameter to `run-test` is a test-machine and the second is a list of commands to execute. The return value from `run-test` is a map with just two keys

```clojure
:results   A sequence of execution results. One for each command attempted
:journal   A snapshot of all kafka output read by the test consumer
```

The journal contains all output written to the topics configured when creating
the test-machine (including any input messages). Each key in the journal
represents one topic. The value is a vector of messages observed on the topic
in the order that they were observed.

### Fixtures

A selection of fixtures are provided to help setting up required topics and
to start the applications, and external systems under test. For more details
see the functions in the [jackdaw.test.fixtures](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT/api/jackdaw.test.fixtures) namespace.

### Wrapping up

You may find it helpful to write a function to tie it all together and invoke
your test function `f` with a machine after performing any setup required. Since
this typically involves some knowledge of the system under test, it's likely
better that you write this macro yourself so that you can tailor it to your own
requirements.

```clojure
(ns my.app-test
  (:require
    [my.app :as app]
    [jackdaw.serdes :refer [string-serde]]
    [jackdaw.test :refer [test-machine]]
    [jackdaw.test.fixtures :refer [with-fixtures topic-fixture service-ready?]])
  (:import
    (org.apache.kafka.streams TopologyTestDriver)))

(def kafka-config
  {"bootstrap.servers" "localhost:9092"
   "group.id" "my-app"})

(def input-topic-config
  {:foo {:topic-name "foo"
         :key-serde (string-serde)
         :value-serde (string-serde)
         :partition-count 1
         :replication-factor 1})

(def output-topic-config
  {:foo {:topic-name "bar"
         :key-serde (string-serde)
         :value-serde (string-serde)
         :partition-count 1
         :replication-factor 1})

(defn with-test-machine
  "Creates a test-machine using the supplied `transport` and then
   passes it to the supplied `f`."
  [f transport]
  (with-fixtures [(topic-fixture kafka-config input-topic-config)
                  (topic-fixture kafka-config output-topic-config)
                  (service-ready? {:http-url "http://localhost:8082"
                                   :timeout 5000})]
    (with-open [machine (test-machine transport)]
      (f machine))))
```

The `topic-fixture` function creates all the topics named in the supplied `topic-config` before running tests. You have to import `TopologyTestDriver` for `test-machine` to work, which requires you to bring in the `org.apache.kafka/kafka-streams-test-utils` library as a dependency, using a version within `2.0.0` - `2.3.0`. The `topic-fixture` expects the `topic-config` to contain `:partition-count` and `:replication-factor` to be present, besides the `topic-name` and key-value serdes.
