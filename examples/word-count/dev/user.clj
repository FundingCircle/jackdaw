(ns user
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [jackdaw.admin :as ja]
            [jackdaw.repl :refer :all]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.describe :as jsd]
            [integrant.core :as ig]
            [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [word-count])
  (:import [clojure.lang ILookup Associative]))


(def config
  {:streams-config {:application-id "word-count"
                    :bootstrap-servers "localhost:9092"
                    :default-key-serde "jackdaw.serdes.EdnSerde"
                    :default-value-serde "jackdaw.serdes.EdnSerde"
                    :cache-max-bytes-buffering "0"}

   :topology {:topology-builder-fn :word-count/topology-builder}

   :topics {:streams-config (ig/ref :streams-config)
            :topology (ig/ref :topology)}

   :app {:streams-config (ig/ref :streams-config)
         :topology (ig/ref :topology)
         :topics (ig/ref :topics)}})


(integrant.repl/set-prep! (constantly config))


(deftype FakeTopicMetadata []
  ILookup
  (valAt [this key]
    {:topic-name (name key)
     :partition-count 1
     :replication-factor 1
     :key-serde (js/edn-serde)
     :value-serde (js/edn-serde)})

  Associative
  (assoc [this key val]
    this))

(defn new-fake-topic-metadata []
  (FakeTopicMetadata.))

(def topic-metadata
  (new-fake-topic-metadata))


(defn topology->topic-metadata
  [topology streams-config]
  (->> (jsd/describe-topology (.build (j/streams-builder* topology)) streams-config)
       (map :nodes)
       (reduce concat)
       (filter #(= :topic (:type %)))
       (remove (fn [x] (re-matches #".*STATE-STORE.*" (:name x))))
       (map :name)
       (reduce (fn [acc x] (assoc acc (keyword x) (get (new-fake-topic-metadata) x))) {})))

(defn re-delete-topics
  "Takes an instance of java.util.regex.Pattern and deletes any Kafka
  topics that match."
  [config re]
  (with-open [client (ja/->AdminClient config)]
    (let [topics-to-delete (->> (ja/list-topics client)
                                (filter #(re-find re (:topic-name %))))]
      (ja/delete-topics! client topics-to-delete))))

(defn destroy-state-stores
  "Takes an application config and deletes local files associated with
  internal state."
  [streams-config]
  (sh "rm" "-rf" (str "/tmp/kafka-streams/" (:application-id streams-config))))


(defmethod ig/init-key :streams-config [_ streams-config]
  (let [bootstrap-servers (or (System/getenv "BOOTSTRAP_SERVERS")
                              (:bootstrap-servers streams-config))]
    (assoc streams-config :bootstrap-servers bootstrap-servers)))

(defmethod ig/init-key :topology [_ {:keys [topology-builder-fn]}]
  (let [topology-builder (deref (find-var (symbol topology-builder-fn)))
        builder (j/streams-builder)]
    ((topology-builder (new-fake-topic-metadata)) builder)))

(defmethod ig/init-key :topics [_ {:keys [streams-config topology] :as opts}]
  (let [config (word-count/propertize (select-keys streams-config [:bootstrap-servers]))
        topic-metadata (topology->topic-metadata topology streams-config)]
    (with-open [client (ja/->AdminClient config)]
      (ja/create-topics! client (vals topic-metadata)))
    (assoc opts :topic-metadata topic-metadata)))

(defmethod ig/init-key :app [_ {:keys [streams-config topology] :as opts}]
  (let [streams-app (j/kafka-streams topology (word-count/propertize streams-config))]
    (j/start streams-app)
    (assoc opts :streams-app streams-app)))


(defmethod ig/halt-key! :topics [_ {:keys [streams-config topic-metadata]}]
  (let [config (word-count/propertize (select-keys streams-config [:bootstrap-servers]))]
    (re-delete-topics config (re-pattern (str "(" (->> topic-metadata
                                                       keys
                                                       (map name)
                                                       (str/join "|"))
                                              ")")))))

(defmethod ig/halt-key! :app [_ {:keys [streams-config streams-app]}]
  (j/close streams-app)
  (destroy-state-stores streams-config)
  (let [config (word-count/propertize (select-keys streams-config [:bootstrap-servers]))]
    (re-delete-topics config (re-pattern (str "("
                                              (:application-id streams-config)
                                              ")")))))
