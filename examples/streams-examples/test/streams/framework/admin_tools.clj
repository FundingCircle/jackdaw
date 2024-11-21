(ns streams.framework.admin-tools
  (:require
   [clojure.tools.logging :as log]
   [clojure.reflect :refer [resolve-class]])
  (:import
   (org.apache.kafka.clients.admin AdminClient ListTopicsResult NewTopic)
   (org.apache.kafka.common KafkaFuture)))

(defn new-topic ^NewTopic [t]
  (doto
    (NewTopic. (:topic-name t)
               (int (:partition-count t))
               (short 1)) ;; Replication factor 1
    (.configs (:config t))))

(defn list-topics ^ListTopicsResult [^AdminClient client]
  (.listTopics client))

(defn create-topics-future
  "Returns a KafkaFuture which wraps the async creation of the topics in the
  supplied map of topics"
  ^KafkaFuture [^AdminClient client topic-config]
  (let [required (->> topic-config
                      (filter (fn [[_ v]]
                                (not (.contains (.get (.names (list-topics client)))
                                                (:topic-name v)))))
                      (map (fn [[_ v]]
                             (new-topic v))))]
    (.all (.createTopics client required))))

(defn create-topics
  "Creates the topics listed in the config against a (remote) <D-d>Kafka Broker from the config"
  [{:keys [streams-settings topics] :as config}]
  (log/info "Creating topics")
  (with-open [client (AdminClient/create
                       {"bootstrap.servers" (:bootstrap-servers streams-settings)
                        "request.timeout.ms" "10000"
                        "client.id" "topic-fixture-admin"})]
    (-> (create-topics-future client topics)
        (.get 10000 java.util.concurrent.TimeUnit/MILLISECONDS))
    (log/info "Created topics OK!")))

(defn- reset-fn
  [^kafka.tools.StreamsResetter rt args]
  (.run rt (into-array String args)))

(defn- class-exists? [c]
  (resolve-class (.getContextClassLoader (Thread/currentThread)) c))

(defn reset-application
  "Runs the kafka.tools.StreamsResetter with the supplied `reset-args` as parameters"
  [{:keys [streams-settings] :as config}]
  (if-not (class-exists? 'kafka.tools.StreamsResetter)
    (throw (RuntimeException. "You must add a dependency on a kafka distrib which ships the kafka.tools.StreamsResetter tool"))
    (let [rt (.newInstance (clojure.lang.RT/classForName "kafka.tools.StreamsResetter"))
          args ["--application-id" (:application-id streams-settings)
                "--bootstrap-servers" (:bootstrap-servers streams-settings)]
          _ (log/info "Resetting application for test")
          result (with-open [out-str (java.io.StringWriter.)
                             err-str (java.io.StringWriter.)]
                   (binding [*out* out-str
                             *err* err-str]
                     (let [status (reset-fn rt args)]
                       (flush)
                       {:status status
                        :out (str out-str)
                        :err (str err-str)})))]
      (if (zero? (:status result))
        (log/info "Application reset OK!")
        (throw (ex-info "failed to reset application. check logs for details" result))))))
