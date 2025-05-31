[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/big-data-europe/Lobby)

# Changes

Version 2.0.0 introduces uses wait_for_it script for the cluster startup

# Hadoop Docker

## Supported Hadoop Versions
See repository branches for supported hadoop versions

## Quick Start

To deploy an example HDFS cluster, run:
```
  docker-compose up
```





`docker-compose` creates a docker network that can be found by running `docker network list`, e.g. `dockerhadoop_default`.

Run `docker network inspect` on the network (e.g. `dockerhadoop_default`) to find the IP the hadoop interfaces are published on. Access these interfaces with the following URLs:

* Namenode: http://<dockerhadoop_IP_address>:9870/dfshealth.html#tab-overview
* History server: http://<dockerhadoop_IP_address>:8188/applicationhistory
* Datanode: http://<dockerhadoop_IP_address>:9864/
* Nodemanager: http://<dockerhadoop_IP_address>:8042/node
* Resource manager: http://<dockerhadoop_IP_address>:8088/

## Configure Environment Variables

The configuration parameters can be specified in the hadoop.env file or as environmental variables for specific services (e.g. namenode, datanode etc.):
```
  CORE_CONF_fs_defaultFS=hdfs://namenode:8020
```

CORE_CONF corresponds to core-site.xml. fs_defaultFS=hdfs://namenode:8020 will be transformed into:
```
  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>
```
To define dash inside a configuration parameter, use triple underscore, such as YARN_CONF_yarn_log___aggregation___enable=true (yarn-site.xml):
```
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
```

The available configurations are:
* /etc/hadoop/core-site.xml CORE_CONF
* /etc/hadoop/hdfs-site.xml HDFS_CONF
* /etc/hadoop/yarn-site.xml YARN_CONF
* /etc/hadoop/httpfs-site.xml HTTPFS_CONF
* /etc/hadoop/kms-site.xml KMS_CONF
* /etc/hadoop/mapred-site.xml  MAPRED_CONF

If you need to extend some other configuration file, refer to base/entrypoint.sh bash script.

# Apache Iceberg Setup with HDFS, Spark, and Hive Metastore

This repository contains a pdf installation guide for configuring Apache Iceberg 1.9.0 with Hadoop Distributed File System (HDFS), Apache Spark 3.4.4, and Hive 4.0.0 Metastore in a Dockerized environment. The guide provides comprehensive instructions for setting up a reproducible big data environment using Docker and Docker Compose, integrating HDFS for storage, Spark for computation, and Hive Metastore for metadata management.

## Features
- **Containerized Environment**: Deploys HDFS, YARN, Spark, Hive Metastore, and PostgreSQL using Docker.
- **Apache Iceberg Integration**: Configures Iceberg for transactional table operations with Hive Metastore and HDFS.
- **Spark and PySpark Support**: Includes instructions for interacting with Iceberg tables via Spark SQL and PySpark.
- **Verification Steps**: Validates setup with MapReduce jobs, Spark version checks, and Iceberg table 

## Prerequisites
- **Docker and Docker Compose**: Installed and configured.
- **PostgreSQL JDBC Driver**: Download `postgresql-42.7.4.jar` from [https://jdbc.postgresql.org/download/](https://jdbc.postgresql.org/download/).
- **System Requirements**: Minimum 8GB RAM recommended for Docker containers.
- **Environment Variables**:
  - `POSTGRES_LOCAL_PATH`: Path to `postgresql-42.7.4.jar` (e.g., `C:/path/to/postgresql-42.7.4.jar`).
  - `HIVE_VERSION`: Set to `4.0.0`.

## Usage
- Setting up HDFS, YARN, Spark, and Hive Metastore services.
- Building custom Spark 3.4.4 images.
- Configuring Iceberg with Spark SQL or PySpark.
- Creating and querying Iceberg tables, and verifying metadata in Hive Metastore and HDFS.

## Airflow-spark Integration

this also includes spark integration with airflow using ssh connection to submit the spark-jobs.
The dockerfile at the main directory can be ignored, its only for using custom airflow image,
which we arenot doing here. 