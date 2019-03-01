# Word Count

This project demonstrates the Jackdaw API by implementing a simple
word-count app complete with e2e tests.

## Setting up

```
$ brew install clojure
$ git clone https://github.com/FundingCircle/jackdaw.git
$ cd jackdaw/examples/word-count
```

## Running Tests

### Unit tests

```
clj -A:test --namespace word-count-test
```

### E2E tests (against local kafka)

```
docker-compose up -d zookeeper kafka
clj -A:test --namespace word-count-e2e-test
```

### E2E tests (against "remote" kafka)

```
export REST_PROXY_URL=http://localhost:8082 
docker-compose up -d rest-proxy
clj -A:test --namespace word-count-e2e-test
```

## Run the app

```
clj -d word-count
```
