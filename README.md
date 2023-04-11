# About Visual Flow

Visual Flow is an ETL tool designed for effective data manipulation via convenient and user-friendly interface. The tool has the following capabilities:

- Can integrate data from heterogeneous sources:
  - DB2
  - IBM COS
  - AWS S3
  - Elastic Search
  - PostgreSQL
  - MySQL/Maria
  - MSSQL
  - Oracle
  - Cassandra
  - Mongo
  - Redis
  - Redshift
- Leverage direct connectivity to enterprise applications as sources and targets
- Perform data processing and transformation
- Run custom code
- Leverage metadata for analysis and maintenance

Visual Flow application is divided into the following repositories: 

- [Visual-Flow-frontend](https://github.com/ibagroup-eu/Visual-Flow-frontend)
- [Visual-Flow-backend](https://github.com/ibagroup-eu/Visual-Flow-backend)
- _**Visual-Flow-jobs**_ (current)
- [Visual-Flow-deploy](https://github.com/ibagroup-eu/Visual-Flow-deploy)
- [Visual-Flow-backend-db-service](https://github.com/ibagroup-eu/Visual-Flow-backend-db-service)
- [Visual-Flow-backend-history-service](https://github.com/ibagroup-eu/Visual-Flow-backend-history-service)

## Visual Flow Jobs Repository

This repository contains two types of jobs:

- Slack job - represents the notification stage in Visual Flow _pipeline_ entity and is written in Python.
- Spark job - represents the Visual Flow _job_ entity and is written in Scala.

**As Visual flow jobs consist of different stages, below is the list of stages that are supported by spark-job:**

- Read stage.
- Write stage.
- Group By stage.
- Remove duplicates stage.
- Filter stage.
- Transformer stage.
- Join stage.
- Change data capture stage.
- Union stage.

For more information on stages check [stage_fields.md](./stage_fields.md)

## Development

[Check the official guide](./DEVELOPMENT.md).

## Contribution

[Check the official guide](https://github.com/ibagomel/Visual-Flow/blob/main/CONTRIBUTING.md)

## License

Visual flow is an open-source software licensed under the [Apache-2.0 license](./LICENSE).
