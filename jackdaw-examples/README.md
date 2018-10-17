# Jackdaw Examples

## Echo Stream

This example is a trivial Kafka topology which simple copies strings from one
topic (`input`) to another topic `output`. You will need to run Kafka
independently:

    $ confluent start kafka

Then start the topology:

    $ lein run -m jackdaw.examples.echo-stream

Start a topic consumer from the command line to see the output:

    $ kafka-console-consumer --bootstrap-server localhost:9092 --topic output

Finally, start a command line producer to send some input:

    $ kafka-console-producer --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:" --topic input

Every string sent to the input topic should appear on the output topic, pluse
the topology will echo the data it sees to its stdout.

