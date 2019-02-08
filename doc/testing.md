# Testing

## TLDR;

```
docker-compose up -d
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

Hopefully any debate about these scenarios ends up as examples in the wiki with
a discussion of the trade-offs so that users can understand them and make the
best choice for their own scenario

In the realm of automated testing scenarios, despite it's rocky past and
uncertain future, `docker-compose` right now gives us the easiest way of
defining and managing a collection of services to be launched as part of a test
no matter the "testing platform" used (e.g. circleci, travis, codebuild).

Some people might find benefit in using it in their dev environments (e.g. as a
final step before submitting a pull request), but the primary reason for it's
inclusion is to ensure test results are reproducible independently of the host
environment. Discussion about how to support any particular workflow seems
likely to generate lots of heat and not much light.

