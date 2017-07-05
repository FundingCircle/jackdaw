# jackdaw-admin

Provides Kafka Admin tools and exposes Zookeeper utils.

jackdaw-admin can be used as a library.

**Kafka Compatibility Warning!**

This project has Kafka dependency. Make sure the kafka used in `project.clj` is compatible with kafka used in your project or environment.

## Usage
Supports functionality:

1. create topic
1. delete topic
1. verify topic existence
1. verify topic existence with retries
1. get partition ids for topic
1. fetch topic metadata
1. exposes `zk-utils`

jackdaw-admin can be used programatically by another application:

**Environment Variables**

| Variable | Default value |
-----------|----------------
| ZOOKEEPER_ADDRESS | "127.0.0.1:2181" |
| BOOTSTRAP_SERVERS | "127.0.0.1:9092" |

Read more about usage in `docs`.

```Clojure
(require '[jackdaw.admin.topic :as topic]
         '[jackdaw.admin.zk :as zk])

(with-open [zk-utils (zk/zk-utils "localhost:2181")]
  (topic/create! zk-utils "foo" 1 1 {}))
  
;; similar for other functions. See tests for examples.
```
