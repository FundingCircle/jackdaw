# kafka.client

A Clojure library providing a very thin wrapper around the main Kafka APIs

## Installation

Add the following to your `:dependencies`

```
[fundingcircle/kafka.client "0.1.4"]
```

## Usage

Here is code example that uses `kafka.client` to send and receive messages:

```clojure
(def timeout 30000)

(defn fuse
  "Returns a function that throws an exception when called after some time has passed."
  [millis]
  (let [end (+ millis (System/currentTimeMillis))]
    (fn []
      (if (< end (System/currentTimeMillis))
        (throw (ex-info "Timer expired" {:millis millis}))
        true))))

(deftest ^:integration customer-account-adapter
  (with-open
    [producer (kafka.client/producer
               test-configs/kafka
               (:kafka-topic/key-serde topics/investor-portal-investors)
               (:kafka-topic/value-serde topics/investor-portal-investors))
     consumer (-> (kafka.client/consumer
                   test-configs/kafka
                   (:kafka-topic/key-serde topics/customer-account)
                   (:kafka-topic/value-serde topics/customer-account))
                  (kafka.client/subscribe (:kafka-topic/topic-name topics/customer-account)))]

    (let [test-msg (finops-fixtures/investor-portal-investor)
          test-customer (:payload test-msg)]

      @(kafka.client/send! producer (kafka.client/producer-record (:kafka-topic/topic-name topics/investor-portal-investors)
                                                                  test-msg))

      (let [[key customer-account] (->> (kafka.client/log-messages consumer timeout (fuse timeout))
                                        (filter (fn [[key msg]]
                                                  (= (:id test-customer)
                                                     (:id msg))))
                                        (first))]
        (is (not (nil? customer-account)))
        (is (= key (:id test-customer)))
        (is (= (:id test-customer) (:id customer-account)))
        (is (= :usa (:geography customer-account)))
        (is (= (:name test-customer) (:description customer-account)))
        (is (= "finops-kstreams/customer-account-adapter" (:published-by customer-account)))
        (is (not (nil? (:tracking-id customer-account))))
        (is (not (nil? (:published-at customer-account))))
        (is (> timeout (time/time-between (time/instant)
                                          (time/instant (:published-at customer-account))
                                          :millis)))))))
```
