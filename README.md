# Jackdaw
<img align="right" src="/etc/jackdaw.jpg" width=300/>

Jackdaw is a Clojure library for the Apache Kafka distributed
streaming platform.

## Libraries

- `jackdaw-admin` - minimal client library for interaction with the Apache
  Kafka management APIs
- `jackdaw-client` - client library for Apache Kafka Producer and Consumer APIs
- `jackdaw-streams` - client library for the Apache Kafka Streams APIs
- `jackdaw-serdes` - utility code for working with various message
  serialisation technologies (Avro, JSON, EDN ...), plus a minimal wrapper for
the Schema Registry.
- `jackdaw-test` - test fixtures for Apache Kafka, Zookeeper, and Confluent
  Schema Registry
- `jackdaw-examples` - some examples of Jackdaw in use

## Installation

To include one of the above libraries, for example `jackdaw-client`
add the following to your `:dependencies`:

    [fundingcircle/jackdaw-client "0.3.0"]

To include all of them:

    [fundingcircle/jackdaw "0.3.0"]

## Building from Source

Jackdaw uses lein modules. To build jackdaw from source, run:

    $ lein modules install

To run the tests, you will need the Confluent platform installed (see
https://docs.confluent.io/current/quickstart/cos-quickstart.html#cos-quickstart):

    $ confluent start kafka
    $ lein modules test

## Using Jackdaw

See the `jackdaw-examples` module for some examples!

## Documentation

 - [Wiki](https://github.com/fundingcircle/jackdaw/wiki)

## Community

<img src="doc/images/slack-icon.png" width="30px"> any questions or
feedback
to [`#jackdaw`](https://fundingcircle.slack.com/messages/jackdaw/)
(or [open an issue](https://github.com/fundingcircle/jackdaw/issues)).

## License

Copyright © 2017 Funding Circle

Distributed under the BSD 3-Clause License.
