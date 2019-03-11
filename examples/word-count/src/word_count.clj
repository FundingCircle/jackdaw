(ns word-count
  "This is the classic 'word count' example done as a stream
  processing application using the Jackdaw Streams API.

  The application reads from a Kafka topic called `input` and splits
  the input value into words. It puts the count on a Kafka topic
  called `output` for each word seen."
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.tools.logging :refer [info]]
   [jackdaw.serdes.edn :as jse]
   [jackdaw.serdes.resolver :as resolver]
   [jackdaw.streams :as j])
  (:gen-class))

(def ^{:const true
       :doc "A topic metadata map.

  Provides all the information needed to create the topics used by the
  application. It also describes the serdes used to read and write to
  the topics."}

  +topic-metadata+

  {:input
   {:topic-name "input"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
    :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}}

   :output
   {:topic-name "output"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
    :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}}})

(def resolve-serde
  (resolver/serde-resolver))

(def topic-metadata
  (reduce-kv (fn [m k v]
               (assoc m k
                      (assoc v
                             :key-serde (resolve-serde (:key-serde v))
                             :value-serde (resolve-serde (:value-serde v)))))
             {}
             +topic-metadata+))

(def app-config
  "Returns the application config."
  {"application.id"            "word-count"
   "bootstrap.servers"         "localhost:9092"
   "default.key.serde"         "jackdaw.serdes.EdnSerde"
   "default.value.serde"       "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})

(defn split-lines
  "Takes an input string and returns a list of words with the
  whitespace removed."
  [input-string]
  (str/split (str/lower-case input-string) #"\W+"))

(defn topology-builder
  "Takes a topic metadata function and returns a function that builds
  the topology."
  [topic-metadata]
  (fn [builder]
    (let [text-input (-> (j/kstream builder (:input topic-metadata))
                         (j/peek (fn [[k v]] (info (str {:key k :value v})))))

          counts (-> text-input
                     (j/flat-map-values split-lines)
                     (j/group-by (fn [[_ v]] v))
                     (j/count))]

      (-> counts
          (j/to-kstream)
          (j/to (:output topic-metadata)))

      builder)))

(defn start-app
  "Starts the stream processing application."
  [topic-metadata app-config]
  (let [builder (j/streams-builder)
        topology ((topology-builder topic-metadata) builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "word-count is up")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "word-count is down"))

(defn -main
  [& _]
  (start-app topic-metadata app-config))


(comment
  ;; You can use this comment block to explore the Word Count
  ;; application.

  ;; This comment block introduces an interactive development
  ;; workflow. With only a few keystrokes, the application can be
  ;; reset, explored, modified, and then reset again [Note 1]. Using
  ;; this workflow, we can treat Kafka Streams apps much like we do
  ;; pure functions. We can supply inputs at the REPL, and we can
  ;; change them and see what they do.

  ;; [Note 1] Stuart Sierra, "My Clojure Workflow, Reloaded"
  ;; http://thinkrelevance.com/blog/2013/06/04/clojure-workflow-reloaded


  ;; STEP 1: Download and Start Confluent Platform

  ;; For serious development, Confluent Platform can be supervised by
  ;; an operating system service manager, e.g., launchd or
  ;; systemd. However, if you want to get up and running quickly,
  ;; download the Confluent CLI from
  ;; `https://www.confluent.io/download/` and add the install location
  ;; to your PATH. Then start Confluent Platform using the Confluent
  ;; CLI `start` command.
  ;; ```
  ;; <path-to-confluent>/bin/confluent start
  ;; ```
  ;;
  ;; The Confluent CLI requires Java 8. If ZooKeeper and Kafka are
  ;; already running, skip this step.


  ;; STEP 2: Launch a Clojure REPL and Call `reset`

  ;; Install Clojure via your favorite package manager, e.g.,
  ;; Homebrew, APT, or DNF. Then change to the Word Count project
  ;; directory and start a REPL.
  ;;
  ;; For example, using Homebrew and the CLI tools:
  ;; ```
  ;; brew install clojure
  ;; cd <path-to-jackdaw>/examples/word-count
  ;; clj
  ;; ```
  ;;
  ;; You should see output like the following:
  ;; ```
  ;; Clojure 1.10.0
  ;; user=>
  ;; ```

  ;; Enter the following at the `user=>` prompt:
  (reset)

  ;; You should see output like the following indicating the topics
  ;; were created and the app is running.
  ;;
  ;; ```
  ;; 23:01:25.939 [main] INFO  system - internal state is deleted
  ;; 23:01:26.093 [main] INFO  word-count - word-count is up
  ;; {:app #object[org.apache.kafka.streams.KafkaStreams 0xb8b2184 "org.apache.kafka.streams.KafkaStreams@b8b2184"]}
  ;; ```

  ;; Emacs users:
  ;;
  ;; Install Cider (https://github.com/clojure-emacs/cider). After
  ;; installing, open a project file, e.g. this one, and use
  ;; `M-x cider-jack-in` to start a REPL. You can evaluate forms using
  ;; `C-x C-e` and `C-c C-v C-f e`. The latter sends output to
  ;; *cider-results*.

  ;; The following `require` is needed because the functons to reset
  ;; app state and produce and consume records are defined in the
  ;; `user` namespace but we want to evaluate them from this one.

  ;; Evaluate the form using `C-x C-e`:
  (require '[user :refer :all])

  ;; Evaluate the form using `C-c C-v C-f e`:
  (reset)

  ;; For the rest of the comment block, it is assumed you can evaluate
  ;; the forms. If you have a basic REPL, use copy-paste.


  ;; STEP 3: Publish Inputs and Get Outputs

  ;; Evaluate the form:
  (publish (:input topic-metadata) nil "all streams lead to kafka")

  ;; Evaluate the form:
  (publish (:input topic-metadata) nil "hello kafka streams")

  ;; Wait approximately five seconds, then evaluate the form:
  (get-keyvals (:output topic-metadata))

  ;; You should see output like the following:
  ;; ```
  ;; (["all" 1]
  ;;  ["streams" 1]
  ;;  ["lead" 1]
  ;;  ["to" 1]
  ;;  ["kafka" 1]
  ;;  ["hello" 1]
  ;;  ["kafka" 2]
  ;;  ["streams" 2])
  ;; ```

  ;; Notice the double occurances of "streams" and "kafka". This
  ;; happens because at an earlier point the count for each of these
  ;; was 'one' and then later became 'two'. To see only the current
  ;; counts, we can transform the result into a map.

  ;; Evaluate the form:
  (->> (get-keyvals (:output topic-metadata))
       (into {})
       (sort-by second)
       reverse)

  ;; You should see output like the following:
  ;; ```
  ;; (["kafka" 2]
  ;;  ["streams" 2]
  ;;  ["hello" 1]
  ;;  ["to" 1]
  ;;  ["lead" 1]
  ;;  ["all" 1])


  ;; This preceding example has a fairly small dataset. Let's make it
  ;; larger.


  ;; The classpath contains a copy of The Metamorphosis. We will load
  ;; and split the dataset by the newlines and publish the fragments
  ;; to `input` topic as separate records. As before, we get the
  ;; counts from the 'output' topic.

  ;; Evaluate the form:
  (reset)

  ;; Evaluate the form:
  (let [text-input (slurp (io/resource "metamorphosis.txt"))
        values (str/split text-input #"\n")]
    (doseq [v values]
      (publish (:input topic-metadata) nil v)
      (info v))
    (info "The End"))

  ;; Wait until the log contains "The End". Then evaluate the form:
  (->> (get-keyvals (:output topic-metadata))
       (into {})
       (sort-by second)
       reverse)

  ;; You should see output like the following:
  ;; ```
  ;; (["the" 1148]
  ;;  ["to" 753]
  ;;  ["and" 642]
  ;;  ["he" 590]
  ;;  ["his" 550]
  ;;  ["of" 429]
  ;;  ["was" 409]
  ;;  ["it" 370]
  ;;  ["had" 352]
  ;;  ["in" 348]
  ;;  ["that" 345]
  ;;  ["gregor" 298]
  ;;  ["a" 285]
  ;;  ["as" 242]
  ;;  ["she" 200]
  ;;  ["with" 199]
  ;;  ["s" 194]
  ;;  ["him" 188]
  ;;  ["her" 187]
  ;;  ["would" 187]
  ;;  ...)
  ;; ```

  )
