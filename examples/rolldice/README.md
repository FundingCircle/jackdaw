# Roll dice

This repo contains a simple example app to demonstrate usage of Jackdaw's Client API (Consumer and Producer API). The app rolls a dice `n` number of times where `n` is provided by the user. A Jackdaw Producer writes the numbers to the input topic `rolldice`. A Jackdaw consumer continuously reads from the topic, adds up the numbers and prints out the result. To exit the consumer loop, press Ctr+c.

## Installation

Clone this repo.

## Usage

1. Bring up the Kafka services by running `docker-compose up -d`. Alternatively, run the Kafka services (broker and zookeeper) locally, following instructions in the [Apache Kafka Quickstart](https://kafka.apache.org/quickstart).

2. From the root directory of the repo, run: `lein run`
