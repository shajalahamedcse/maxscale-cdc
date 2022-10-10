# Kafka Maxscale CDC Connector

Stream Maxscale CDC events to Kafka.

## Run

```bash
go run main.go \
-cdc-host=127.0.0.1 \
-cdc-port=4001 \
-cdc-user=maxuser \
-cdc-password=maxpwd \
-cdc-database=test \
-cdc-table=names \
-cdc-uuid=e3835094-4824-4f9d-ba97-5e2ec217733b \
-kafka-brokers=localhost:29092 \
-kafka-topic=cdc-test-names \
-datadir=/tmp \
-initial-delay=1s \
-v=2
```

## Sample SQL

```sql
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
CREATE TABLE test.names(id INT auto_increment PRIMARY KEY, name VARCHAR(20));
INSERT INTO test.names (name) VALUES ('Hello');
INSERT INTO test.names (name) VALUES ('World');
```

## Consome topic

```bash
kafka-console-consumer --topic cdc-test-names --bootstrap-server localhost:9092

```  