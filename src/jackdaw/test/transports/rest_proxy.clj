(ns jackdaw.test.transports.rest-proxy
  (:require
   [aleph.http :as http]
   [byte-streams :as bs]
   [clojure.data.json :as json]
   [clojure.core.async :as async]
   [clojure.tools.logging :as log]
   [jackdaw.client :as kafka]
   [jackdaw.test.commands :as cmd]
   [jackdaw.test.journal :as j]
   [jackdaw.test.transports :as t]
   [jackdaw.test.serde :refer :all]
   [jackdaw.test.transports.kafka :refer [mk-producer-record]]
   [manifold.deferred :as d])
  (:import
   (java.util UUID Base64)))


(def ok? #{200 204})

(defn uuid
  []
  (str (UUID/randomUUID)))

(defn- base64-encode
  [encodable]
  (let [encoder (Base64/getEncoder)]
    (-> (.encode encoder encodable)
        (String.))))

(defn- base64-decode
  [decodable]
  (let [decoder (Base64/getDecoder)]
    (.decode decoder decodable)))

(defn datafy-record
  [{:keys [producer-record] :as m}]
  (let [data (-> (select-keys m [:key :value])
                 (assoc :topic (get-in m [:topic :topic-name]))
                 (assoc :partition (.partition producer-record))
                 (update :key base64-encode)
                 (update :value base64-encode))]
    (assoc m :data-record data)))

(defn undatafy-record
  [topic-metadata m]
  (-> m
      (update :key base64-decode)
      (update :value base64-decode)
      (assoc :topic (j/reverse-lookup topic-metadata (:topic m)))))

(def content-types
  {:byte-array "application/vnd.kafka.binary.v2+json"
   :json "application/vnd.kafka.v2+json"})

(defn rest-proxy-headers
  [accept]
  {"Content-Type" "application/vnd.kafka.binary.v2+json"
   "Accept"       accept})

(defn handle-proxy-request
  [method url headers body]
  (let [req {:throw-exceptions? false
             :headers headers
             :body (when body
                     (json/write-str body))}

        accept? (fn [id]
                  (= (content-types id)
                     (get headers "Accept")))

        content-available? (fn [response]
                             (not (= 204 (:status response))))]

    (log/debug "> " method url (select-keys req [:status :headers :body]))

    ;; This is an asynchronous pipeline we use to process responses
    ;; from the rest-proxy. It feels a bit like `->` macro but each
    ;; step automatically waits for the promise returned by the
    ;; previous step to be delivered before using it to produce a new
    ;; promise for the next step.
    ;;
    ;; We use it here to just `assoc` on the results of attempting to
    ;; parse the json response unless the request returns a 204. If
    ;; the request returns an error, we parse it (it should still be
    ;; JSON), and stick it in `:error`. `:json-body` will remain as
    ;; nil in this case.

    (d/chain (method url req)
      #(update % :body bs/to-string)
      #(do (log/debug "< " method url (select-keys % [:status :body]))
           %)
      #(assoc % :json-body (when (content-available? %)
                             (json/read-str (:body %)
                                            :key-fn (comp keyword
                                                          (fn [x]
                                                            (clojure.string/replace x "_" "-"))))))
      #(if-not (ok? (:status %))
         (assoc % :error :proxy-error)
         %))))

(defn destroy-consumer
  [{:keys [base-uri]}]
  (let [url base-uri
        headers {"Accept" (content-types :json)}
        body nil]
    (let [response @(handle-proxy-request http/delete url headers body)]
      (when (:error response)
        (throw (ex-info "Failed to destroy consumer after use" {}))))))

(defn topic-post
  [{:keys [bootstrap-uri]} msg callback]
  (let [url (format "%s/topics/%s"
                    bootstrap-uri
                    (:topic msg))
        headers {"Accept" (content-types :json)
                 "Content-Type" (content-types :byte-array)}
        body {:records [(select-keys msg [:key :value :partition])]}

        response @(handle-proxy-request http/post url headers body)]

    (let [record (when-not (:error response)
                   (-> (get-in response [:json-body :offsets])
                       first
                       (assoc :topic (:topic msg))))]
      (callback record (when (:error response)
                         response)))))

(defrecord RestProxyClient [bootstrap-uri
                            base-uri
                            group-id
                            instance-id
                            subscription])

(defn proxy-client-info [client]
  (let [object-id (-> (.hashCode client)
                      (Integer/toHexString))]
    (-> (select-keys client [:bootstrap-uri])
        (assoc :id object-id))))

(defn rest-proxy-client
  [config]
  (map->RestProxyClient config))

(defn with-consumer
  [{:keys [bootstrap-uri group-id] :as client}]
  (let [id (uuid)
        url (format "%s/consumers/%s"
                    bootstrap-uri
                    group-id)
        headers {"Accept" (content-types :json)
                 "Content-Type" (content-types :json)}
        body {:name id
              :auto.offset.reset "latest"
              :auto.commit.enable false}
        preserve-https (fn [consumer]
                         ;; Annoyingly, the proxy will return an HTTP address for a
                         ;; subscriber even when its running over HTTPS
                         (if (clojure.string/starts-with? url "https")
                           (update consumer :base-uri clojure.string/replace #"^http:" "https:")
                           consumer))]

    (let [response @(handle-proxy-request http/post url headers body)]
      (if (:error response)
        (do (log/infof "rest-proxy create consumer error: %s" (:error response))
            (assoc client :error response))
        (let [{:keys [base-uri instance-id]} (:json-body response)]
          (preserve-https
            (assoc client :base-uri base-uri, :instance-id instance-id)))))))

(defn with-subscription
  [{:keys [base-uri group-id instance-id] :as client} topic-metadata]
  (let [url (format "%s/subscription" base-uri)
        topics (map :topic-name (vals topic-metadata))
        headers {"Accept" (content-types :json)
                 "Content-Type" (content-types :json)}
        body {:topics topics}]
    (let [response @(handle-proxy-request http/post url headers {:topics topics})]
      (if (:error response)
        (do (log/infof "rest-proxy subscription error: %s" (:error response))
            (assoc client :error response))
        (assoc client :subscription topics)))))

(defn rest-proxy-poller
  "Returns a function that takes a consumer and puts any messages retrieved
   by polling it onto the supplied `messages` channel"
  [messages]
  (fn [consumer]
    (let [{:keys [base-uri group-id instance-id]} consumer
          url (format "%s/records" base-uri)
          headers {"Accept" (content-types :byte-array)}
          body nil]
      (let [response @(handle-proxy-request http/get url headers body)]
        (when (:error response)
          (log/errorf "rest-proxy fetch error: %s" (:error response)))
        (when (not (:error response))
          (doseq [msg (:json-body response)]
            (async/onto-chan messages [msg] false)))))))

(defn rest-proxy-subscription
  [config topic-metadata]
  (-> (rest-proxy-client config)
      (with-consumer)
      (with-subscription topic-metadata)))

(defn rest-proxy-consumer
  "Creates an asynchronous Kafka Consumer of all topics defined in the
   supplied `topic-metadata`

   Puts all messages on the channel in the returned response. It is the
   responsibility of the caller to arrange for the read the channel to
   be read by some other process.

   Must be closed with `close-consumer` when no longer required"
  [config topic-metadata deserializers]
  (let [continue?   (atom true)
        xform       (comp
                     #(apply-deserializers deserializers %)
                     #(undatafy-record topic-metadata %))
        messages    (async/chan 1 (map xform))
        started?    (promise)
        poll        (rest-proxy-poller messages)]

    {:process (async/go-loop [consumer (rest-proxy-subscription config topic-metadata)]
                (poll consumer)

                (when-not (realized? started?)
                  (deliver started? true)
                  (log/infof "started rest-proxy consumer: %s" (proxy-client-info consumer)))

                (if @continue?
                  (do (poll consumer)
                      (Thread/sleep 500)
                      (recur consumer))
                  (do
                    (async/close! messages)
                    (destroy-consumer consumer)
                    (log/infof "stopped rest-proxy consumer: %s" (proxy-client-info consumer)))))
     :started? started?
     :messages messages
     :continue? continue?}))

(defn build-record
  "Builds a Kafka Producer and assoc it onto the message map"
  [m]
  ;; The easiest way to make this as real as possible is to build
  ;; a real ProducerRecord and then get extract the data from it. This way
  ;; we don't have to calculate the partition oureslves.
  ;;
  ;; On the other hand, this means we have a bit of an unwanted dependency
  ;; (in this ns) on the Kafka Client API. Perhaps later it might be
  ;; worth calculating this partition ourselves based on the supplied
  ;; topic-metadata but there are a few wrinkles to iron out with that
  ;; approach.
  (let [rec (mk-producer-record (:topic m)
                                (:partition m)
                                (:timestamp m)
                                (:key m)
                                (:value m))]
    (assoc m :producer-record rec)))

(defn deliver-ack
  "Deliver the `ack` promise with the result of attempting to write to kafka. The
   default command-handler waits for this before on to the next command so the
   test response may indicate the success/failure of each write command."
  [ack]
  (fn [rec-meta ex]
    (when-not (nil? ack)
      (if ex
        (deliver ack {:error :send-error
                      :message (:message ex)})
        (deliver ack (select-keys rec-meta
                                  [:topic :offset :partition
                                   :serialized-key-size
                                   :serialized-value-size]))))))

(defn rest-proxy-producer
  "Creates an asynchronous kafka producer to be used by a test-machine for for
   injecting test messages"
  ([config topics serializers]
   (let [producer       (rest-proxy-client config)
         xform          (comp
                         datafy-record
                         build-record
                         #(apply-serializers serializers %))
         messages       (async/chan 1 (map xform))]

     (log/infof "started rest-proxy producer: %s" producer)

     (async/go-loop [{:keys [data-record ack serialization-error] :as m} (async/<! messages)]
       (cond
         data-record       (do (log/debug "sending data: " data-record)
                               (topic-post producer data-record (deliver-ack ack))
                               (recur (async/<! messages)))
         serialization-error   (do (deliver ack {:error :serialization-error
                                                 :message (.getMessage serialization-error)})
                                   (recur (async/<! messages)))
         :else (do
                 (log/infof "stopped rest-proxy producer: %s" producer))))

     {:producer  producer
      :messages  messages})))

(defmethod t/transport :confluent-rest-proxy
  [{:keys [config topics]}]
  (let [serdes        (serde-map topics)
        test-consumer (rest-proxy-consumer config topics (get serdes :deserializers))
        test-producer (when @(:started? test-consumer)
                        (rest-proxy-producer config topics (get serdes :serializers)))]
    {:consumer test-consumer
     :producer test-producer
     :serdes serdes
     :topics topics
     :exit-hooks [(fn []
                    (async/close! (:messages test-producer)))
                  (fn []
                    (reset! (:continue? test-consumer) false)
                    (async/<!! (:process test-consumer)))]}))
