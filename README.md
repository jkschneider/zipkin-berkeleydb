## Purpose

This is a Lucene/MapDB storage module for [Zipkin](http://zipkin.io). It is intended for use where the storage of roughly tens of millions of spans is needed, Zipkin needs to consume relatively little heap, and you don't want the complexity of running Cassandra or Elastic.

The goal of this module is to pair Zipkin with Prometheus, the Prometheus RSocket Proxy, and Grafana on a single node 1 vCPU + 4GB RAM Kubernetes cluster on a public cloud provider and still be able to ingest tens millions of traces and many thousands of metrics, while keeping operational costs low.

## Running

```bash
$ docker run -d -p 9411:9411 jkschneider/zipkin-lucene-mapdb
```

## Releasing

Ensure there is a `gradle.properties` file with properties `dockerUser` and `dockerPassword` and then run:

```bash
./gradlew final
```
