(ns jackdaw.streams
  "Kafka streams protocols."
  (:refer-clojure :exclude [count map reduce group-by merge filter])
  (:import org.apache.kafka.streams.KafkaStreams
           org.apache.kafka.streams.processor.TopologyBuilder))
