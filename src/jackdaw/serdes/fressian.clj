(ns jackdaw.serdes.fressian
  "Implements a Fressian SerDes (Serializer/Deserializer)."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.data.fressian :as fressian]
            [jackdaw.serdes.fn :as jsfn])
  (:import java.io.ByteArrayOutputStream
           org.fressian.FressianWriter)
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serde]
   :prefix "FressianSerde-"
   :name jackdaw.serdes.FressianSerde))

(set! *warn-on-reflection* true)

(defn- fressian-bytes
  [obj options]
  (with-open [bos                    (ByteArrayOutputStream.)
              ^FressianWriter writer (apply fressian/create-writer bos options)]
    (fressian/write-object writer obj)
    (fressian/write-footer writer)
    (.toByteArray bos)))

(defn fressian-serializer
  "Returns a Fressian serializer. Takes the same options as
  `clojure.data.fressian/create-writer`."
  [& options]
  (jsfn/new-serializer {:serialize (fn [_ _ data]
                                     (when data (fressian-bytes data options)))}))

(defn fressian-deserializer
  "Returns a Fressian deserializer. Takes the same options as
  `clojure.data.fressian/read`."
  [& options]
  (jsfn/new-deserializer {:deserialize (fn [_ _ data]
                                         (apply fressian/read data options))}))

(def FressianSerde-configure
  (constantly nil))

(defn FressianSerde-serializer
  [& _]
  (fressian-serializer))

(defn FressianSerde-deserializer
  [& _]
  (fressian-deserializer))
