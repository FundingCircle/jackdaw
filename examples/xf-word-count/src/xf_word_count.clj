(ns xf-word-count
  "This is the classic 'word count' example done using transducers."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j]
            [jackdaw.streams.xform :as jxf]))


(defn xf-running-total
  [state swap-fn]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (let [[k v] input
             next (as-> v %
                    (swap-fn state #(merge-with (fnil + 0) %1 %2) %)
                    (select-keys % (keys v))
                    (map vec %))]
         (rf result next))))))

(defn xf
  [state swap-fn]
  (comp
   (map (fn [[k v]] [k (str/split v #" ")]))
   (map (fn [[k v]] [k (frequencies v)]))
   (xf-running-total state swap-fn)))


(comment
  ;; Use this comment block to explore Word Count using Clojure
  ;; transducers.

  ;; Launch a Clojure REPL:
  ;; ```
  ;; cd <path-to-jackdaw>/examples/xf-word-count
  ;; clj -A:dev
  ;; ```

  ;; Emacs users: Open a project file, e.g. this one, and enter
  ;; `M-x cider-jack-in`.

  ;; Evaluate the form:
  (def coll
    [["1" "inside every large program"]
     ["2" "is a small program"]
     ["3" "struggling to get out"]])

  ;; Let's counts the words. Evaluate the form:
  (transduce (xf (atom {}) swap!) concat coll)

  ;; You should see output like the following:

  ;; (["inside" 1]
  ;;  ["every" 1]
  ;;  ["large" 1]
  ;;  ["program" 1]
  ;;  ["is" 1]
  ;;  ["a" 1]
  ;;  ["small" 1]
  ;;  ["program" 2]
  ;;  ["struggling" 1]
  ;;  ["to" 1]
  ;;  ["get" 1]
  ;;  ["out" 1])


  ;; This time, let's count the words using
  ;; `jackdaw.streams.xform/fake-kv-store` which implements the
  ;; KeyValueStore interface with overrides for get and put."

  ;; Evaluate the form:
  (transduce (xf (jxf/fake-kv-store {}) jxf/kv-store-swap-fn) concat coll)

  ;; You should see the same output.
  )


(def streams-config
  {"application.id" "xf-word-count"
   "bootstrap.servers" (or (System/getenv "BOOTSTRAP_SERVERS") "localhost:9092")
   "cache.max.bytes.buffering" "0"})

(defn topology-builder
  [{:keys [input output] :as topics} xf]
  (fn [builder]
    (jxf/add-state-store! builder)
    (-> (j/kstream builder input)
        (jxf/transduce-kstream xf)
        (j/to output))
    builder))


(comment
  ;; Use this comment block to explore Word Count as a stream
  ;; processing application.

  ;; For more details on dynamic development, see the comment block in
  ;; <path-to-jackdaw>/examples/word-count/src/word_count.clj

  ;; Start ZooKeeper and Kafka:
  ;; ```
  ;; <path-to-directory>/bin/confluent local start kafka
  ;; ```

  ;; Launch a Clojure REPL:
  ;; ```
  ;; cd <path-to-jackdaw>/examples/xf-word-count
  ;; clj -A:dev
  ;; ```

  ;; Emacs users: Open a project file, e.g. this one, and enter
  ;; `M-x cider-jack-in`.

  ;; Evaluate the form:
  (reset)

  ;; Evaluate the form:
  (let [coll ["inside every large program"
              "is a small program"
              "struggling to get out"]]
    (doseq [x coll]
      (publish (:input topic-metadata) nil x)))

  ;; Evaluate the form:
  (get-keyvals (:output topic-metadata))

  ;; You should see output like the following:

  ;; (["inside" 1]
  ;;  ["every" 1]
  ;;  ["large" 1]
  ;;  ["program" 1]
  ;;  ["is" 1]
  ;;  ["a" 1]
  ;;  ["small" 1]
  ;;  ["program" 2]
  ;;  ["struggling" 1]
  ;;  ["to" 1]
  ;;  ["get" 1]
  ;;  ["out" 1])
  )
