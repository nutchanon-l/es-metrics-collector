# Elasticsearch Metrics Collector
The script is used for collect important Elasticsearch metrics for monitor health of Elasticsearch cluster.

The source and target clusters must be Elasticsearch cluster.
So, you can provision target cluster as a centralized monitoring cluster, and separate source cluster by using alias in the configuration file.

## Prerequisites
* Python 3.6+
* Python packages
```
$ pip install -r requirements.txt
```

## How-To
1. Modify "source" and "target" Elasticsearch endpoint configuration.
2. Modify "alias" as name of source cluster.
3. Run python script every minute (more or less).
```
$ python es-metrics-collector.py
```
4. (Optional) Define cron job for schedule the collector.
5. (Optional) Build Kibana or Grafana dashboard on top of data for better visualization.

## Elasticsearch metrics
Metrics | Elasticsearch API | Description
------------ | ------------- | ------------- 
Cluster health | /_cluster/health | Overview of Elasticsearch health
Node metrics | /_nodes/stats | Node statistics include read, write, thread, memory, JVM
Indices metrics | /_all/_stats | Index statistics include read, write, memory
Indices status | /_cat/indices | Index status include health, shard, size
Shard allocation | /_cat/allocation | Shard allocation include number of shard, disk size

## Elasticsearch version
Version | Support
------------ | -------------
7.9 | Not Supported
7.8 | Supported
7.7 | Supported
6.8 | Supported

## Configuration
* source.alias = Custom value for "alias" field at root document. The value is used for seperate source cluster.
* source.readTimeout = Read timeout in seconds when collect metrics from source cluster.
* target.timezoneDiff = Define automatic timezone configuration, FALSE when target cluster is version 7.0+
