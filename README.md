[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/big-data-europe/Lobby)
# Hadoop-Spark-Hive Stack (Docker Compose)

This repository contains a Docker Compose setup for a Hadoop + Spark + Hive stack. The `docker-compose.yaml` defines the following services:

1. **Apache Hadoop**  
   - **NameNode** (HDFS metadata)  
   - **DataNode** (HDFS data)  
   - **ResourceManager** (YARN)  
   - **NodeManager** (YARN)  
   - **HistoryServer** (YARN application history)

2. **Apache Spark**  
   - **Spark Master**  
   - **Spark Worker**  
   - **Spark History Server**

3. **Apache Hive**  
   - **PostgreSQL** (Hive Metastore backend)  
   - **Hive Metastore** (Thrift service)  
   - **HiveServer2** (JDBC/ODBC queries)

---

## Table of Contents

1. [Repository Structure](#repository-structure)   
2. [Launching the Stack](#launching-the-stack)  
3. [Accessing Services](#accessing-services)  
4. [Stopping & Cleaning Up](#stopping--cleaning-up)  
5. [Volume Persistence](#volume-persistence)  
6. [Adding JDBC Drivers for Hive](#adding-jdbc-drivers-for-hive)  

---

## Repository Structure

```text
.
├── datanode/                    ← DataNode helper scripts
│   └── run.sh
├── historyserver/               ← Spark HistoryServer helper scripts
│   └── run.sh
├── jars/                        ← External JARs (e.g., JDBC drivers)
│   └── postgresql-42.7.4.jar
├── logs/                        ← Shared logs for Hadoop/Spark/Hive
├── namenode/                    ← NameNode helper scripts
│   └── run.sh
├── nodemanager/                 ← NodeManager helper scripts
│   └── run.sh
├── resourcemanager/             ← ResourceManager helper scripts
│   └── run.sh
├── spark-custom-images/         ← Custom Spark images (Master, Worker, History)
│   ├── Dockerfile.master
│   ├── Dockerfile.worker
│   └── Dockerfile.history
├── spark-events/                ← Spark event logs for HistoryServer
├── docker-compose.spark.yaml          ← Main Compose file (Hadoop + Spark + Hive)
├── hadoop.env                   ← Environment variables for Hadoop/YARN containers
├── .env                         ← Environment variables for Docker Compose (image tags, UIDs)
└── README.md                    ← This file
```

## Launching the Stack

1. **Clone the Repository**
   ```bash
   git clone https://github.com/DixT-180/HDFS-HIVE-SPARK-AIRFLOW.git
   cd HDFS-HIVE-SPARK-AIRFLOW
   ```

2. **Build Custom Spark Images**  
   If you want to build your own Spark Master, Worker, and History Server images, navigate to `spark-custom-images/` and run:
   ```bash
   cd spark-custom-images
   docker build -t custom-spark-master:3.5.5 -f Dockerfile.master .
   docker build -t custom-spark-worker:3.5.5 -f Dockerfile.worker .
   docker build -t custom-spark-history:3.5.5 -f Dockerfile.history .
   cd ..
   ```
   Skip if these images are already available.

3. **Start All Services**
   ```bash
   docker-compose up -d
   ```
   - Pulls any missing images and starts containers defined in `docker-compose.yaml`.

4. **Verify Container Status**
   ```bash
   docker-compose ps
   ```
   Confirm each service is `Up (healthy)` (especially NameNode, ResourceManager, Spark Master).

---

## Accessing Services

After starting, access services via `localhost`:

| Service                        | Port | URL / Endpoint                                      |
|--------------------------------|------|-----------------------------------------------------|
| **HDFS NameNode Web UI**       | 9870 | http://localhost:9870                               |
| **YARN ResourceManager Web UI**| 8088 | http://localhost:8088                               |
| **Spark Master Web UI**        | 8081 | http://localhost:8081                               |
| **Spark Worker Web UI**        | 8082 | http://localhost:8082                               |
| **Spark History Server**       | 18080| http://localhost:18080                              |
| **PostgreSQL (Hive Metastore)**| 5432 | `postgres://hive:password@localhost:5432/metastore_db` |
| **Hive Metastore Thrift**      | 9083 | Thrift endpoint for Hive Metastore                  |
| **HiveServer2 (Thrift)**       | 10000| JDBC/ODBC endpoint for Hive queries                 |

> If ports conflict on your host, edit the `ports:` mapping in `docker-compose.yaml` before launching.

---

## Stopping & Cleaning Up

1. **Stop All Containers (keep volumes)**
   ```bash
   docker-compose down
   ```

2. **Stop and Remove Everything (including volumes)**
   ```bash
   docker-compose down --volumes
   ```
   - Deletes named volumes (`hadoop_namenode`, `hadoop_datanode`, `hadoop_historyserver`, `hive-db`, `warehouse`).  
   - **Warning:** This removes persisted data (HDFS metadata, Hive DB).

---

## Volume Persistence

Named volumes in `docker-compose.yaml`:

- **`hadoop_namenode`**  
  - Mounted at `/hadoop/dfs/name` in the NameNode  
  - Stores HDFS metadata

- **`hadoop_datanode`**, **`hadoop_datanode2`**, **`hadoop_datanode3`**  
  - Mounted at `/hadoop/dfs/data` in each DataNode  
  - Stores HDFS block data

- **`hadoop_historyserver`**  
  - Mounted at `/hadoop/yarn/timeline` in the HistoryServer  
  - Stores YARN application logs

- **`hive-db`**  
  - Mounted at `/var/lib/postgresql/data` in PostgreSQL  
  - Stores Hive Metastore DB

- **`warehouse`**  
  - Mounted at `/opt/hive/data/warehouse` in Hive containers  
  - Hive warehouse for tables/data

Host-mounted directories:

- `./jars` → `/opt/spark/external-jars` (external JAR files)
- `./spark-events` → `/spark-events` (Spark event logs)
- `./logs` → shared logs for Hadoop/Spark/Hive

---

## Adding JDBC Drivers for Hive

Hive requires a JDBC driver (PostgreSQL) for its Metastore:

1. **Place the JDBC JAR in `./jars/`**  
   Example:
   ```
   jars/postgresql-42.7.4.jar
   ```

2. **Bind-mount into the Hive Metastore service** in `docker-compose.spark.yaml`:
   ```yaml
   metastore:
     image: apache/hive:4.0.0
     ...
     volumes:
       - warehouse:/opt/hive/data/warehouse
       - type: bind
         source: ./jars/postgresql-42.7.4.jar
         target: /opt/hive/lib/postgres.jar
   ```

3. **Restart Hive Metastore**:
   ```bash
   docker-compose restart metastore
   ```


