(ns user
  (:require [system :refer :all]))


(comment
  ;;; Evaluate the forms.
  ;;;

  ;; Start ZooKeeper and Kafka, e.g. by running
  ;; `docker-compose up -d` from this example project's root folder.

  ;; Create the `input` and `output` topics, and start the app.
  (start)

  ;; Get a list of current topics.
  (list-topics)

  ;; Write to the input stream.
  (publish {:k1 "Some value" :k2 "Some other value"})

  ;; Read from the output stream.
  (consume))
