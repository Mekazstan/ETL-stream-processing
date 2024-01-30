# Realtime (Scalable) Stream Processing & Orchestration
An end to end Data Engineering project using technologies such as Apache Kafka, Apache Spark, cassandra DB all orchestrated with Apache Airflow & containerized with Docker

It contains an airflow DAG that fetches data from the Random User Generator API (https://randomuser.me/) intermitiently this data is then streamed into Kafka managed by Apache Zookeeper which is then streamed by Spark using a Master worker architecture which then streams it into cassandra all this are managed in a docker container. 
