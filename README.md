<img align="left" src="doc/images/jackdaw-logo.png" width=125/>

Jackdaw is a Clojure library for the Apache Kafka distributed streaming platform. With Jackdaw, you can create and list topics using the AdminClient API, produce and consume records using the Producer and Consumer APIs, and create stream processing applications using the Streams API. Jackdaw also contains functions to serialize and deserialize records as JSON, EDN, and Avro, as well as functions for writing unit and integration tests.


## Installation

To use the latest release, add the following to your project:

[![Clojars Project](https://img.shields.io/clojars/v/fundingcircle/jackdaw.svg)](https://clojars.org/fundingcircle/jackdaw)

## Documentation

- [API docs](https://fundingcircle.github.io/jackdaw/codox/index.html)


## Examples

- [Pipe](https://github.com/FundingCircle/jackdaw/tree/master/examples/pipe)
- [Word Count](https://github.com/FundingCircle/jackdaw/tree/master/examples/word-count)
- [Simple Ledger](https://github.com/FundingCircle/jackdaw/tree/master/examples/simple-ledger)

## Testing

The included docker-compose file (adapted from confluent's ["all-in-one"](https://github.com/confluentinc/cp-docker-images/blob/5.1.0-post/examples/cp-all-in-one/docker-compose.yml) compose file)
provides all the services required to run the project's tests. This allows the tests
to be run as follows. You'll need up-to-date versions of docker, docker-compose and lein

```
docker-compose up -d
docker run \
   --env-file .env.circle \
   --workdir /usr/src/app \
   --mount type=bind,source=${HOME}/.m2,destination=/root/.m2 \
   --mount type=bind,source=$(pwd),destination=/usr/src/app \
   --network jackdaw_default \
  clojure:openjdk-8-lein lein test
```

## Contributing

We welcome any thoughts or patches. You can reach us in [`#jackdaw`](https://clojurians.slack.com/messages/CEA3C7UG0/) (or [open an issue](https://github.com/fundingcircle/jackdaw/issues)).


## License

Copyright © 2017 Funding Circle

Distributed under the BSD 3-Clause License.
