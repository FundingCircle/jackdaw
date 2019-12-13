(ns pipe
  "This example creates a simple stream processing application using transducers."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.xform :as jxf]
            [jackdaw.streams.xform.fakes :as fakes]))


(defn xf
  [state swap-fn]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (let [[k v] input
             next [[k v]]]
         (rf result next))))))


(comment
  ;; Use this comment block to explore Pipe using Clojure transducers.

  ;; Launch a Clojure REPL:
  ;; ```
  ;; cd <path-to-jackdaw>/examples/pipe
  ;; clj -A:dev
  ;; ```

  ;; Emacs users: Open a project file, e.g. this one, and enter
  ;; `M-x cider-jack-in`.

  ;; Evaluate the form:
  (def input [["1" "foo"] ["2" "bar"]])

  ;; Let's record the entries. Evaluate the form:
  (transduce (xf (atom {}) swap!) concat input)

  ;; You should see output like the following:

  ;; (["1" "foo"] ["2" "bar"])
  )


(def streams-config
  {"application.id" "pipe"
   "bootstrap.servers" (or (System/getenv "BOOTSTRAP_SERVERS") "localhost:9092")
   "cache.max.bytes.buffering" "0"})

(defn topology-builder
  [{:keys [input output] :as topics} xforms]
  (fn [builder]
    (jxf/add-state-store! builder)
    (-> (j/kstream builder input)
        (jxf/transduce (::xf xforms))
        (j/to output))
    builder))


(comment
  ;; Use this comment block to explore Pipe as a stream processing
  ;; application.

  ;; For more details on dynamic development, see the comment block in
  ;; <path-to-jackdaw>/examples/word-count/src/word_count.clj

  ;; Start ZooKeeper and Kafka:
  ;; ```
  ;; <path-to-directory>/bin/confluent local start kafka
  ;; ```

  ;; Launch a Clojure REPL:
  ;; ```
  ;; cd <path-to-jackdaw>/examples/pipe
  ;; clj -A:dev
  ;; ```

  ;; Emacs users: Open a project file, e.g. this one, and enter
  ;; `M-x cider-jack-in`.

  ;; Evaluate the form:
  (reset)

  ;; Evaluate the form:
  (let [input [["1" "foo"] ["2" "bar"]]]
    (doseq [[k v] input]
      (publish (:input topic-metadata) k v)))

  ;; Evaluate the form:
  (get-keyvals (:output topic-metadata))

  ;; You should see output like the following.

  ;; (["1" "foo"] ["2" "bar"])
  )
