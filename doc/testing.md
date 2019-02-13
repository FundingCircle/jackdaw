# Testing

## TLDR;

```
lein test
```

## More Detail

The test-suite has some integration tests that assume the presence of a few of
the services you might expect. Currently the services required by the tests
are:-

 - zookeeper
 - kafka
 - schema-registry
 - rest-proxy

As developers, we have a veritable feast of options available to us about
exactly how these services are made available to the test-suite. For example,
your OS probably has a few mechanisms for doing it. Confluent has another.
Docker, nomad, mesosphere....You get the point.

Each has pros and cons and developers optimizing for different work-flows can
reasonably disagree about best practices for their particular scenario.

Here we describe a few ways to get a test cluster up and running in order to
be able to run the tests.

### Confluent Platform

For the most up-to-date releases, installation and configuration instructions,
consider going straight to the source

https://docs.confluent.io/current/quickstart/ce-quickstart.html#ce-quickstart


### Docker/Docker Compose

Check out the [docker](docker/) directory for an example docker-compose file
containing all the required services.

```
cp doc/docker/docker-compose.*.yml .
docker-compose up -d
lein test
```

One thing to bear in mind about the docker approach is that it can be difficult
to share configuration and test-data between multiple projects in a micro-services
eco-system.

The above solutions are a great way to get the required services up and running
with minimal effort. If you'd like deeper integration with your operating system
so that you can setup things like monitoring, log rotation, continue reading...

### OSX

TODO launchd configurations...

### Linux

TODO systemd config?

