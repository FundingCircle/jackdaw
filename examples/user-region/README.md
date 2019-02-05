# User Region

This tutorial contains a simple stream processing application using Jackdaw and Kafka Streams.
It demonstrates group-by operations and aggregations on KTable by computing the number of users 
per region.

## Setting up

Before starting, it is recommended to install the Confluent Platform CLI which can be obtained from [https://www.confluent.io/download/](https://www.confluent.io/download/).

To install Clojure: [https://clojure.org/guides/getting_started](https://clojure.org/guides/getting_started).
To install Leiningen: [https://leiningen.org/](https://leiningen.org/).

## Project structure

The project structure looks like this:
```
$ tree user-region
user-region
├── dev
│   └── system.clj
├── src
│   └── user_region
│       └── core.clj
├── README.md
├── project.clj
```

The `project.clj` file describes the project's dependencies and source paths.

The `dev/system.clj` file contains functions to start, stop, and reset the app. These are required 
by the `user` namespace for interactive development and should not be invoked directly.

The `user_regionn/core.clj` file describes the app and topology. The app reads from a Kafka 
topic called 'user-region', computes the number of users per region, and writes the counts to a 
topic called  'region-user-count'. 

The topology uses a KTable to the number of users per region.
```
(defn topology-builder
  [builder]
  (let [user-region-table (j/ktable builder (topic-config "user-region"))

        region-count (-> user-region-table
                         (j/group-by (fn [[_ v]] (vector v v))
                                     (topic-config nil
                                                   (Serdes/String)
                                                   (Serdes/String)))
                         (j/count))]

    (-> region-count
        (j/to-kstream)
        (j/to (topic-config "region-user-count")))

    builder))

```