(ns streams.core
  (:require
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [jackdaw.serdes.resolver :as jd-resolver]
   [jackdaw.streams :as js]
   [streams.config :refer [config]]
   [topology-grapher.describe :as td]
   [topology-grapher.render :as tr])
  (:import
   (jackdaw.streams.interop CljStreamsBuilder)
   (java.time Duration)
   (java.util Properties)
   (org.apache.kafka.streams.errors StreamsUncaughtExceptionHandler)
   (org.apache.kafka.streams KafkaStreams KafkaStreams$State KafkaStreams$StateListener StreamsBuilder Topology)))


;; Config

(defn load-config []
   (config))

(defn props-for ^Properties [config-map]
  (doto (Properties.)
    (.putAll (reduce-kv (fn [m k v]
                          (assoc m (-> (name k)
                                       (string/replace "-" "."))
                                 (str v)))
                        {} config-map))))

;; Some Jackdaw serdes sugar

(defn- resolve-serde
  [schema-reg-url schema-reg-client serde-config]
  (reduce (fn [conf serdes-key]
            (assoc conf serdes-key ((jd-resolver/serde-resolver
                                     :schema-registry-url schema-reg-url
                                     :schema-registry-client schema-reg-client)
                                    (serdes-key serde-config))))
          serde-config [:key-serde :value-serde]))

(defn reify-serdes-config
  "Converts the serdes references in the topic & store configs into
  actual jackdaw serdes implementations, so the config can be used
  in the jackdaw API calls."
  ([config]
   (reify-serdes-config config nil))
  ([config schema-reg-client]
   ;; If no schema-reg-client is provided, Jackdaw will construct
   ;; one as needed using the :streams-settings :schema-registry-url in the kafka config
   ;; for the app (see config.edn).
   ;; Typically a schema-reg-client is passed here for testing (to pass a mock)
   (let [schema-reg-url (get-in config [:streams-settings :schema-registry-url])
         topics (into {} (map (fn [[k t]]
                                [k (resolve-serde schema-reg-url schema-reg-client t)])
                              (:topics config)))
         stores (into {} (map (fn [[k s]]
                                [k (resolve-serde schema-reg-url schema-reg-client s)])
                              (:stores config)))
         global-stores (into {} (map (fn [[k v]]
                                       (if-let [resolved-topic (get topics (:source-topic v))]
                                         [k (assoc v :source-topic resolved-topic)]
                                         (throw (ex-info (str "Source topic not found for global store " k) v))))
                                     (:global-stores config)))]
     (assoc config
            :topics topics
            :stores stores
            :global-stores global-stores))))

;; Some Running-a-topology Sugar

(defn exit-on-error-handler ^StreamsUncaughtExceptionHandler []
  (reify StreamsUncaughtExceptionHandler
    (handle [_ ex]
      (try
       (future (System/exit 1))
       (finally (throw ex))))))

(defn logging-error-handler ^StreamsUncaughtExceptionHandler []
  (reify StreamsUncaughtExceptionHandler
    (handle [_ ex]
      (log/error ex "Kafka Streams error!"))))

(defn startup-listener [startup-result]
  (reify KafkaStreams$StateListener
    (onChange [_ new-state old-state]
      (when-not (realized? startup-result)
        (cond
         (= KafkaStreams$State/RUNNING new-state)
         (deliver startup-result :running)
         (or (= KafkaStreams$State/ERROR new-state)
             (= KafkaStreams$State/PENDING_SHUTDOWN new-state))
         (deliver startup-result :failed)
         :else nil)))))

;; Main interfaces to build and run a topology

(defn build-topology
  "Given one of the various ways of building a topology object for
  kafka streams, returns a built Topology."
  [stream-build-fn config]
  (let [configured-stream (stream-build-fn config)]
    (condp instance? configured-stream
      ;; If we built a Topology already, e.g use of the Processor API, just return it
      Topology configured-stream
      ;; Jackdaw Streams DSL, wrapped streams builder
      CljStreamsBuilder (.build ^StreamsBuilder (js/streams-builder* configured-stream))
      ;; Kafka Streams DSL
      StreamsBuilder (.build ^StreamsBuilder configured-stream)
      ;; Erk
      (throw (Exception. (str "Unknown builder type: " (type configured-stream)))))))

(defn start-topology
  "Given a topology and the streams config to run it, create and run a
  KafkaStreams for ever and ever. Accepts optional state listener and
  exception handler."
  (^KafkaStreams [^Topology topology config]
   (start-topology topology config nil))
  (^KafkaStreams [^Topology topology config extra-setup-fn]
   (let [kafka-streams (KafkaStreams. ^Topology topology
                                      (props-for (:streams-settings config)))]
     (when extra-setup-fn
       (extra-setup-fn kafka-streams))
     (.start kafka-streams)
     (.addShutdownHook (Runtime/getRuntime)
                       (Thread. (let [main-thread (Thread/currentThread)]
                                  (fn []
                                    (.close kafka-streams (Duration/ofSeconds 60))
                                    (.join main-thread 45000)))
                                "Shutdown thread"))
     kafka-streams)))

;;
;; topology rendering
;;
(def topology-domain-meta {:domain "jackdaw"
                           :subdomain "examples"
                           :application "stream"})

(defn render-topology [stream-build-fn stream-config]
  (let [topology [{:application-name (get-in stream-config [:streams-settings :application-id])
                   :topology (build-topology stream-build-fn stream-config)}]
        graph (td/gen-topologies topology topology-domain-meta)]
    {:topic-level (tr/render-graph (vals graph) {:fmt "pdf" :mode "topics" :cache false})
     :detail (tr/render-graph (vals graph) {:fmt "pdf" :mode "detail" :cache false})}))

;; The whole app config is passed through the application here.
;; This config contains:
;; - the kafka streams configuration
;; - the topic configurations, in jackdaw format
;; - the state store configurations, in jackdaw format
;; All serdes should be reified here

(defn run-topology ^KafkaStreams [stream-build-fn stream-config]
  (let [startup-result (promise)
        error-handler-fn (if (true? (:hard-exit-on-error stream-config))
                           exit-on-error-handler
                           logging-error-handler)
        stream (-> (build-topology stream-build-fn stream-config)
                   (start-topology stream-config
                                       (fn [^KafkaStreams kafka-streams]
                                         (doto kafka-streams
                                           (.setUncaughtExceptionHandler
                                             ^StreamsUncaughtExceptionHandler (error-handler-fn))
                                           (.setStateListener
                                             (startup-listener startup-result))))))]
    (let [startup-state @startup-result]
      (if (= :running startup-state)
        (log/info (:streams-settings stream-config) "Started topology")
        (throw (ex-info "Failed to start topology :(" {:state startup-state
                                                       :config stream-config}))))
    stream))
