(ns jackdaw.test.fixtures
  ""
  (:require
   [aleph.http :as http]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [jackdaw.client :as kafka]
   [jackdaw.streams :as k]
   [jackdaw.streams.interop :refer [streams-builder]]
   [jackdaw.test.transports.kafka :as kt]
   [jackdaw.test.serde :refer [byte-array-serializer byte-array-deserializer]]
   [manifold.deferred :as d]
   [clojure.test :as t])
  (:import
   (org.apache.kafka.clients.admin AdminClient NewTopic)
   (org.apache.kafka.streams KafkaStreams$StateListener)
   (kafka.tools StreamsResetter)))

;;; topic-fixture ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- new-topic
  [t]
  (doto (NewTopic. (:topic-name t)
                   (:partition-count t)
                   (:replication-factor t))
    (.configs (:config t))))

(defn list-topics
  [client]
   (.listTopics client))

(defn- create-topics
  "Creates "
  [client kafka-config topic-config]
  (let [required (->> topic-config
                      (filter (fn [[k v]]
                                (not (.contains (-> (list-topics client)
                                                    .names
                                                    .get)
                                                (:topic-name v)))))
                      (map (fn [[k v]]
                             (new-topic v))))]
    (-> (.createTopics client required)
        (.all))))

(defn topic-fixture
  "Returns a fixture function that creates all the topics named in the supplied
   topic config before running a test function."
  ([kafka-config topic-config]
   (topic-fixture kafka-config topic-config 10000))

  ([kafka-config topic-config timeout-ms]
   (fn [t]
     (with-open [client (AdminClient/create kafka-config)]
       (-> (create-topics client kafka-config topic-config)
           (.get timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS))
       (log/info "topic-fixture: created topics: " (keys topic-config))
       (t)))))

;;; application reset ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn delete-files-recursively [fname & [silently]]
  (letfn [(delete-f [file]
            (when (.isDirectory file)
              (doseq [child-file (.listFiles file)]
                (delete-f child-file)))
            (clojure.java.io/delete-file file silently))]
    (delete-f (clojure.java.io/file fname))))

(defn reset-application [app-config]
  (fn [t]
    (let [rt (StreamsResetter.)
          app-id (get app-config "application.id")
          app-state (format "%s/%s"
                            (or (get app-config "state.dir")
                                "/tmp/kafka-streams")
                            app-id)]
      (when (.exists (io/as-file app-state))
        (delete-files-recursively app-state))
      (log/info "deleted app state")
      (let [args (->> ["--application-id" (get app-config "application.id")
                       "--bootstrap-servers" "localhost:9092"]
                      (into-array String))]

        (if (zero? (.run rt args))
          (t)
          (throw (ex-info "failed to reset application. check logs for details" {})))))))

;;; skip-to-end ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- class-name
  [instance]
  (-> (.getClass instance)
      (.getName)))

(defn skip-to-end
  "Returns a fixture that skips to the end of the supplied topic before running
   the test function"
  [{:keys [topic config]}]
  (fn [t]
    (let [config (assoc config
                        "key.serializer" (class-name byte-array-serializer)
                        "key.deserializer" (class-name byte-array-deserializer)
                        "value.serializer" (class-name byte-array-serializer)
                        "value.deserializer" (class-name byte-array-deserializer))]
      (doto (kt/subscription config [topic])
        (.commitSync)
        (.close))

      (log/infof "skipped to end: %s" (:topic-name topic))

      (t))))

;;; kstream-fixture ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- set-started
  [app-id started?]
  (reify KafkaStreams$StateListener
    (onChange [_ new-state old-state]
      (log/infof "process %s changed state from %s -> %s"
                 app-id
                 (.name old-state)
                 (.name new-state))
      (when-not (realized? started?)
        (when (.isRunning new-state)
          (deliver started? true))))))

(defn- set-error
  [error]
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ t e]
      (log/error e (.getMessage e))
      (reset! error e))))

(defn kstream-fixture
  "Returns a fixture that builds and starts kafka streams for the supplied topology
   before running the test function (and then tears it down when the test is
   complete).

   `compose-fixtures` or `join-fixtures` may be used to build fixtures combine
   topologies"
  [{:keys [topology config]}]
  (fn [t]
    (let [builder (k/streams-builder)
          stream (k/kafka-streams (topology builder) config)
          error (atom nil)
          started? (promise)]

      (.setUncaughtExceptionHandler stream (set-error error))
      (.setStateListener stream (set-started (get config "application.id") started?))

      (k/start stream)

      (when @started?
        (log/info "commencing test function"))

      (try
        (t)
        (finally
          (k/close stream)
          (log/infof "closed stream: %s" (get config "application.id"))
          (when @error
            (log/error @error (.getMessage @error))
            (throw (ex-info (str "Uncaught exception: " (.getMessage @error))
                            {:config config
                             :stream stream}
                            @error))))))))

;; system readyness

(defn service-ready?
  [{:keys [http-url http-params timeout]}]
  (fn [t]
    (let [ok? (fn [x]
                (and (not (= :timeout x))
                     (= (:status 200))))

          ready-check @(d/timeout!
                        (d/future
                          (loop []
                            (if-let [result (try
                                              @(http/get http-url http-params)
                                              (catch java.net.ConnectException _))]
                              result
                              (recur))))
                        timeout
                        :timeout)]
      (if (ok? ready-check)
        (t)
        (throw (ex-info (format "service %s not available after waiting for %s"
                                http-url
                                timeout)
                        {}))))))

(defmacro with-fixtures
  [fixtures & body]
  `((t/join-fixtures ~fixtures)
    (fn []
      ~@body)))
