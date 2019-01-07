(ns jackdaw.test.transports.identity
  (:require
   [clojure.tools.logging :as log]
   [clojure.core.async :as async]
   [jackdaw.test.journal :as j]
   [jackdaw.test.transports :as t]))

(defn identity-consumer
  [ch]
  (let [started? (promise)]
    {:started? started?
     :messages ch}))

(defn identity-producer
  []
  (let [messages (async/chan 1)]
    {:messages messages}))

(defmethod t/transport :identity
  [{:keys []}]
  (let [ch (async/chan 1)
        test-consumer (identity-consumer ch)
        test-producer (identity-producer)
        p (async/pipe (:messages test-producer)
                      (:messages test-consumer))]
    {:consumer test-consumer
     :producer test-producer
     :exit-hooks [(fn []
                    (async/close! (:messages test-producer))
                    (async/<!! p))]}))
