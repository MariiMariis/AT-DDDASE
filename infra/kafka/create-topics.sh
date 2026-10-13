#!/bin/sh
set -e
BOOTSTRAP=kafka:19092
TOPICS=/opt/kafka/bin/kafka-topics.sh

criar() {
  $TOPICS --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

criar contrato.eventos 3

$TOPICS --bootstrap-server $BOOTSTRAP --describe --topic contrato.eventos
