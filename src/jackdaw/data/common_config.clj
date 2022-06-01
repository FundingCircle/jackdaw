(in-ns 'jackdaw.data)

(import '[org.apache.kafka.common.config
          ConfigResource ConfigResource$Type])

(set! *warn-on-reflection* true)

;;; ConfigResource.Type

(def +broker-config-resource-type+
  ConfigResource$Type/BROKER)

(def +topic-config-resource-type+
  ConfigResource$Type/TOPIC)

(def +unknown-config-resource-type+
  ConfigResource$Type/UNKNOWN)

(defn ->ConfigResourceType [o]
  (case o
    :config-resource/broker +broker-config-resource-type+
    :config-resource/topic +topic-config-resource-type+
    +unknown-config-resource-type+))

(defn->data ConfigResourceType->data
  [^ConfigResource$Type crt]
  (cond (= +broker-config-resource-type+ crt)
        :config-resource/broker

        (= +topic-config-resource-type+ crt)
        :config-resource/topic

        :else
        :config-resource/unknown))

;;; ConfigResource

(defn ->ConfigResource
  [^ConfigResource$Type type ^String name]
  (ConfigResource. type name))

(defn ->topic-resource
  [name]
  (->ConfigResource +topic-config-resource-type+ name))

(defn ->broker-resource
  [name]
  (->ConfigResource +broker-config-resource-type+ name))

(defn->data ConfigResource->data
  [^ConfigResource cr]
  {:name (.name cr)
   :type (datafy (.type cr))})
