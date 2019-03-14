# Jackdaw &middot; [![Clojars Project](https://img.shields.io/clojars/v/fundingcircle/jackdaw.svg)](https://clojars.org/fundingcircle/jackdaw) [![Code Coverage](https://codecov.io/gh/FundingCircle/jackdaw/branch/master/graph/badge.svg)](https://codecov.io/gh/FundingCircle/jackdaw) [![cljdoc badge](https://cljdoc.org/badge/fundingcircle/jackdaw)](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT) [![CircleCI](https://circleci.com/gh/FundingCircle/jackdaw.svg?style=shield)](https://circleci.com/gh/FundingCircle/jackdaw)

Jackdaw is a Clojure library for the Apache Kafka distributed streaming platform. With Jackdaw, you can create and list topics using the AdminClient API, produce and consume records using the Producer and Consumer APIs, and create stream processing applications using the Streams API. Jackdaw also contains functions to serialize and deserialize records as JSON, EDN, and Avro, as well as functions for writing unit and integration tests.

# Supported versions

Jackdaw currently only works with Clojure >= 1.10.
This is because we are using the `datafy` protocol which was only introduced in 1.10.

## Documentation

You can find all the documentation on [cljdoc](https://cljdoc.org/d/fundingcircle/jackdaw).

## Examples

- [Pipe](https://github.com/FundingCircle/jackdaw/tree/master/examples/pipe)
- [Word Count](https://github.com/FundingCircle/jackdaw/tree/master/examples/word-count)
- [Simple Ledger](https://github.com/FundingCircle/jackdaw/tree/master/examples/simple-ledger)

## Contributing

We welcome any thoughts or patches. You can reach us in [`#jackdaw`](https://clojurians.slack.com/messages/CEA3C7UG0/) (or [open an issue](https://github.com/fundingcircle/jackdaw/issues)).


## License

Copyright © 2017 Funding Circle

Distributed under the BSD 3-Clause License.
