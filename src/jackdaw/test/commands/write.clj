(ns jackdaw.test.commands.write
  (:require
   [clojure.spec.alpha :as s]
   [clojure.core.async :as async]
   [clojure.tools.logging :as log]))

(s/def ::write-implicit-key
  (s/cat :timeout pos-int?
         :topic string?
         :msg any?
         :timestamp (s/nilable integer?)))

(s/def ::write-explicit-key
  (s/cat :timeout pos-int?
         :topic string?
         :k any?
         :v any?
         :timestamp (s/nilable integer?)))

(s/def ::write-params
  (s/alt
   :implict-key ::write-implicit-key
   :explicit-key ::write-explicit-key))

(comment
  (s/conform ::write-params [1000 "foo" "yolo" nil])
  (s/conform ::write-params [1000 "foo" "k" "yolo" nil]))

(defn write-explicit-key
  "Returns the result of writing `msg` using `producer`"
  [{:keys [producer serdes]}
   {:keys [topic k v timestamp timeout]}]
   (let [{:keys [messages]} producer
         ack (promise)]
     (async/put! messages
                 {:topic topic
                  :key k
                  :value v
                  :timestamp (or timestamp (System/currentTimeMillis))
                  :ack ack})
     (deref ack timeout {:error :timeout})))

(defn write-implicit-key
  [{:keys [producer serdes]}
   {:keys [topic msg timestamp timeout]}]
  (let [{:keys [messages]} producer
        ack (promise)
        msg-key (get msg (keyword (:unique-key topic)))]
    (async/put! messages
                {:topic topic
                 :key msg-key
                 :value msg
                 :timestamp (or timestamp (System/currentTimeMillis))
                 :ack ack})
    (deref ack timeout {:error :timeout})))

(defn write
  [machine write-args]
  (let [writers {:explicit-key write-explicit-key
                 :implict-key write-implicit-key}
        [writer args] write-args]
    ((get writers writer) machine args)))

(defn invalid?
  [x]
  (= x :clojure.spec.alpha/invalid))

(defn handle-write-cmd
  [machine cmd params]
  (let [parsed-params (s/conform ::write-params params)
        invalid? (= :clojure.spec.alpha/invalid parsed-params)

        [write-type {:keys [topic] :as write-args}] (when-not invalid?
                                                      parsed-params)
        t (when topic
            (get (:topics machine) topic))]

    (log/info "write-type: " write-type)
    (log/info "write-args: " (assoc write-args :topic t))

    (cond
      invalid? {:error :invalid-params
                :explain-data (s/explain-data ::write-params params)}

      t        (write machine [write-type (assoc write-args
                                                 :topic t)])
      :else    {:error :unknown-topic
                :topic topic
                :known-topics (keys (:topic-config machine))})))

(def command-map
  {:jackdaw.test.commands/write! handle-write-cmd})
