(ns kafka.streams.fixtures
  (:require [kafka.streams.topology :refer [start-topologies]]))

;; Begin Hack
;;
;; Remove once https://issues.apache.org/jira/browse/KAFKA-4369 is fixed
;; or zookeeper is removed from kafka. Whichever comes first.

(defn ^:private private-field [obj field]
  (let [m (.. obj getClass (getDeclaredField (name field)))]
    (. m (setAccessible true))
    (. m (get obj))))

(defn ^:private shutdown-kafka-4369 [streams]
  (doseq [t (private-field streams :threads)]
    (let [spa (private-field t :partitionAssignor)
          topic-mgr (private-field spa :internalTopicManager)]
      (.close (private-field topic-mgr :zkClient)))))

;; End Hack

(defn kstreams-fixture
  "Returns a fixture that creates the topologies and starts stream processing."
  [topology-builders kafka-configs]
  (fn [t]
    (let [streams (start-topologies topology-builders kafka-configs)]
      (try
        (t)
        (finally
          (.close streams)
          (shutdown-kafka-4369 streams))))))
