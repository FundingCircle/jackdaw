# jackdaw-example

This repo contains a simple example app to demonstrate usage of Jackdaw's Client API (Consumer and Producer API). The app rolls a dice `n` number of times where `n` is provided by the user. A Jackdaw Producer writes the numbers to the input topic `rolldice`. A Jackdaw consumer continuously reads from the topic, adds up the numbers and prints out the result. To exit the consumer loop, press Ctr+c.

## Installation

Clone this repo.

## Usage

1. Bring up the Kafka services by running `docker-compose up -d`. Alternatively, run the Kafka services (broker and zookeeper) locally, following instructions in the [Apache Kafka Quickstart](https://kafka.apache.org/quickstart).

2. From the root directory of the repo, run: `lein run`

## License

Copyright © 2021 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
