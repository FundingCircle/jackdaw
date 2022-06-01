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
- [Roll Dice](https://github.com/FundingCircle/jackdaw/tree/master/examples/rolldice)

## Contributing

We welcome any thoughts or patches. You can reach us in [`#jackdaw`](https://clojurians.slack.com/messages/CEA3C7UG0/) (or [open an issue](https://github.com/fundingcircle/jackdaw/issues)).

## Related projects

If you want to get more insight about your topologies, you can use the
[Topology Grapher](https://github.com/FundingCircle/topology-grapher) library to generate graphs.
See [an example using jackdaw](https://github.com/FundingCircle/topology-grapher/blob/master/sample_project/src/jackdaw_topology.clj) to check how to integrate it with your topology.

## Releasing

Anyone with the appropriate credentials can "cut a release" of jackdaw using the following steps.

 1. Review the diff of master vs the latest released tag (e.g. while preparing 0.7.0, I looked at https://github.com/FundingCircle/jackdaw/compare/0.6.9...master to see what was actually merged vs what was in the Changelog). Make a PR to put a date on the version being released and if necessary ensure completeness and consistency of the Changelog
 2. Use the [Draft a new release](https://github.com/FundingCircle/jackdaw/releases/new) feature in github to prepare a release
 3. In the "tag version" field, enter the proposed version
 4. In the "release title" field, enter "v[version]"
 5. In the "describe this release" field, enter the contents of the Changelog and add a credit to the contributors of the release
 6. When happy, use the "Publish Release" button to publish the release in github which creates a corresponding git tag
 7. Once the tag is seen by circleci, a deployment build is triggered which builds the project and deploys a release to clojars

Steps 2 to 6 is essentially `git tag $version -m "$title\n\n$description" && git push --tags`


## License

Copyright © 2017 Funding Circle

Distributed under the BSD 3-Clause License.
