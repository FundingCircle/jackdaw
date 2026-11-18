(ns streams.config)

(def ^:private base-event-topic-config
  {:partition-count 3
   :key-serde {:serde-keyword :jackdaw.serdes/string-serde}
   :value-serde {:serde-keyword :jackdaw.serdes.json/serde}})

(def ^:private base-changelog-topic-config
  {:partition-count 3
   :key-serde {:serde-keyword :jackdaw.serdes/string-serde}
   :value-serde {:serde-keyword :jackdaw.serdes.json/serde}
   ;; Changelog
   :config {"cleanup.policy" "compact"
            "segment.ms" "86400000"}})

(defn config []
  {:app-name "streams"

   :streams-settings
   {:schema-registry-url "http://localhost:8081"
    :bootstrap-servers "localhost:9092"
    :commit-interval "1"
    :num-stream-threads "3"
    :cache-max-bytes-buffering  "0"
    :replication-factor "1"
    :state-dir "/tmp/kafka-streams"
    :sidecar-port "9090"
    :sidecar-host "0.0.0.0"}

   :topics
   {:input (merge {:topic-name "input-1"} base-event-topic-config)
    :output (merge {:topic-name "output-1"} base-event-topic-config)
    :state (merge {:topic-name "state-1"} base-changelog-topic-config)}

   :stores
   {:state-store {:store-name "state-store-1"
                  :key-serde {:serde-keyword :jackdaw.serdes/string-serde}
                  :value-serde {:serde-keyword :jackdaw.serdes.json/serde}}}

   :global-stores {}})
