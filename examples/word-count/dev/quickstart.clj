(ns quickstart
  "doc-string"
  (:require [user :refer :all])
  (:import [org.apache.kafka.common.serialization Serdes]))


(comment

  ;; comment
  (confluent/start)

  ;; comment
  (get-topics)

  ;; comment
  (start)

  ;; comment
  (get-topics)

  ;; comment
  (publish (topic-config "input") nil "all streams lead to kafka")

  ;; comment
  (publish (topic-config "input") nil "hello kafka streams")

  ;; comment
  (get-keyvals (topic-config "input"))

  ;; comment
  (get-keyvals (topic-config "output"))

  ;; comment
  (get-records (topic-config "output"))

  ;; comment
  (get-keyvals (topic-config (str "word-count-"
                                  "KSTREAM-AGGREGATE-STATE-STORE-0000000004-"
                                  "changelog")
                             (Serdes/String)
                             (Serdes/Long)))

  ;; replace (j/counts) with (j/counts (topic-config "counts"))
  ;; comment
  (get-keyvals (topic-config "word-count-counts-changelog"))

  ;; comment
  (reset)

  ;; comment
  (stop)

  ;; comment
  (get-topics)

  ;; comment
  (confluent/stop)

  )
