confluent start kafka

lein run -m jackdaw.examples.echo-stream

kafka-console-producer --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:" --topic input

kafka-console-consumer --bootstrap-server localhost:9092 --topic output
