# Jackdaw AdminClient API

## Rational

The Jackdaw Admin API wraps Kafka's `AdminClient` <sup>[1](#adminapi)</sup> API. It provides functions for obtaining information about a Kafka cluster and managing its topics.

## AdminClient

All functions in the `jackdaw.admin` namespace require a `client`. To produce it, pass the `jackdaw.admin/->AdminClient` function a `config` map. At a minimum, the configuration<sup>[2](#admin-configs)</sup> needs to contain the property `bootstrap.servers` set to the address of a running Kafka broker:

```clojure
(ns adminclient-example
  (:require [jackdaw.admin :as ja]))

(def client (ja/->AdminClient {"bootstrap.servers" "localhost:9092"}))
```

## Managing topics

### Creating topics

To create a topic, use the `create-topics!` function, which takes a client, as well as an array of topic descriptor maps. The map needs to contain the mandatory keys `:topic-name`, `:partition-count`, and `:replication-factor`. Additionally, the map may contain an optional `:topic-config` key, holding the configuration<sup>[3](#topic-configs)</sup> of the topic as stringified key-value pairs.

```clojure
(ja/create-topics! client [{:topic-name "jackdaw"
                            :partition-count 15
                            :replication-factor 3
                            :topic-config {"cleanup.policy" "compact"}}])
```

### Viewing topics

To simply list the names of the topics in the cluster, use the `list-topics` function:

```clojure
(ja/list-topics client) ;; => ({:topic-name "jackdaw"})
```

To view the partition information for a topic, use `describe-topics` function:

```clojure
(ja/describe-topics client [{:topic-name "jackdaw"}])
;; => {"jackdaw" {:is-internal? false,
;;                :partition-info ({:isr [{:host "127.0.0.1", :id 0, :port 9092, :rack nil}],
;;                                  :leader {:host "127.0.0.1", :id 0, :port 9092, :rack nil},
;;                                  :partition 0,
;;                                  :replicas [{:host "127.0.0.1", :id 0, :port 9092, :rack nil}]})}}
```

Finally, to get the full topic configuration, use `describe-topic-configs`:

```clojure
(ja/describe-topics-configs client [{:topic-name "jackdaw"}])
;; => {{:name "jackdaw", :type :config-resource/topic} {"cleanup.policy" {:default? false,
;;                                                                        :name "cleanup.policy",
;;                                                                        :read-only? false,
;;                                                                        :sensitive? false,
;;                                                                        :value "compact"},
;;                                                                                     ...}}
```

### Changing topics' configuration

To change the configuration of a topic, pass the function `alter-topic-config!` a client and a vector of topic descriptor maps. Each map needs to contain two keys - the `:topic-name`, as well as a `:topic-config`, holding the configuration<sup>[3](#topic-configs)</sup> of the topic as stringified key-value pairs.

```clojure
(ja/alter-topic-config! client [{:topic-name "jackdaw"
                                 :topic-config {"delete.retention.ms" "1000"}}])
```

### Deleting topics

```clojure
(ja/delete-topics! client [{:topic-name "jackdaw"}])
```

## Other utilities

There are several functions, , such as `describe-cluster`, `get-broker-config`, and `partition-ids-of-topics`, for obtaining information about various aspects of the Kafka cluster.

## References

<a name="adminapi">1</a>: https://kafka.apache.org/documentation/#adminapi <br />
<a name="admin-configs">2</a>: https://docs.confluent.io/current/installation/configuration/admin-configs.html <br />
<a name="topic-configs">3</a>: https://docs.confluent.io/current/installation/configuration/topic-configs.html <br />
