# playground-kafka-streams

This repository contains code that I was using to fiddle around
with [Kafka Streams](https://kafka.apache.org/documentation/streams/).

## How to use

1. Start a local Kafka with `docker compose up`.
2. Execute `maven verify`
3. Execute the main methods of the classes/use-cases you're interested in.

## CountryCounter

### What it does

* Thread 1 creates Kafka records with a randomly picked name of a German federal state and publishes that record to a
  KStreams source topic.
* Thread 2 uses a Kafka streaming topology to count the occurrence of each name.
* Thread 3 consumes from the KStreams sink topic and prints the federal state's name and its occurrences.

### What it looks like

```
08:12:21 d.h.p.kafkastreams.CountryCounter - thüringen occurred 921 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - saarland occurred 832 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - rheinland-pfalz occurred 844 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - nordrhein-westfalen occurred 901 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - bremen occurred 935 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - sachsen occurred 920 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - hamburg occurred 887 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - mecklenburg-vorpommern occurred 874 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - bayern occurred 884 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - schleswig-holstein occurred 916 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - baden-württemberg occurred 879 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - brandenburg occurred 916 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - niedersachsen occurred 899 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - berlin occurred 860 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - hessen occurred 885 times.
08:12:21 d.h.p.kafkastreams.CountryCounter - sachsen-anhalt occurred 884 times.
```