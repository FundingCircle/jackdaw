export ZOOKEEPER_CONNECT=zookeeper:2181
export BOOTSTRAP_SERVERS=kafka:9092
export SCHEMA_REGISTRY_URL=http://schema-registry:8081

alias consumer="kafka-console-consumer --zookeeper $ZOOKEEPER_CONNECT"
alias producer="kafka-console-producer --broker-list $BOOTSTRAP_SERVERS"
alias topic="kafka-topics --zookeeper $ZOOKEEPER_CONNECT"

function avro-consumer () {
    kafka-avro-console-consumer --zookeeper $ZOOKEEPER_CONNECT \
                                --property schema.registry.url=$SCHEMA_REGISTRY_URL $@
}

## topics

function describe-topic () {
    topic --describe --topic $1
}

function create-topic () {
    topic --create --topic $1 $@
}

function list-topics () {
    topic --list
}

## consumers
function avro-consume () {
    t=$1 && shift
    avro-consumer --topic $t $@
}

function consume () {
    t=$1 && shift
    consumer --topic $t $@
}
