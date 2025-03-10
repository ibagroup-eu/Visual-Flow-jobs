## Spark Job Stages parameters
*Stage* is element of execution in Spark Job. 
Execution order is defined by graph with *nodes* connected by *edges*.
Each node is stage configured with its own set of input parameters, identified with unique *id* that provided in JSON format.

### Agreements
* field names are case-sensitive string
* field values are case-sensitive strings
* fields are mandatory by default

### This document agreements
* field values in angle brackets ('<' and '>') should be provided by user
* texts starting with '//' are comments and not present in actual requests
* *column1(,column2(,..))* means one or more entries separated by comma, e.g. 
  * *column1*
  * *column1,column2*
  * *column1,column2,column3*

### READ stages
#### Read generator
```
{
  "id": "<id>"
  "value": {
    "operation": "READ",
    "storage": "GENERATOR",
    "alias": "<alias>" // alias of resulting Spark DataFrame 
  }
}
```
#### Read JDBC
```
{
  "id": "<id>",
  "value": {
    "operation": "READ",
    "storage": "<storage>", // supported values: 'db2', 'sqlite', 'oracle', 'mysql', 'postgresql', 'mssql', 'redshift-jdbc'
    "jdbcUrl": "jdbc:<storage>:<parameters>",
    "schema": "<database schema>",
    "table": "<database table name>",
    "user": "<credentials: user>",
    "password": "<credentials: password>",
    "schema": "<database schema>", // optional
    "certData": "<db2 certificate>", // optional
  }
}
```
#### Read object storage
```
{
  "id": "<id>",
  "value": {
    "operation": "READ",
    "storage": "<storage>", // supported values: 'cos', 's3', 'azure-blob-storage', 'google-cloud-storage'
    "accessKey": "<access key>", // credentials
    "secretKey": "<secret key>", // credentials
    "bucket": "<bucket>", // for 'cos', 's3', 'google-cloud-storage'
    "container": "<container>", // for 'azure-blob-storage'
    "path": "<path>", // path in bucket, for 'cos', 's3', 'google-cloud-storage'
    "containerPath": "<path>", // path in container, for 'azure-blob-storage'
    "format": "<format>", // Spark DataFrame format, e.g. 'csv', 'parquet', 'json', etc.
    "endpoint": "<endpoint>" // only for 'cos' and 's3'
  }
}
```
#### Read elastic
```
{
  "id": "<id>",
  "value": {
    "operation": "READ",
    "storage": "elastic",
    "nodes": "<nodes>", // elasticsearch nodes to connect to (separated by comma)
    "port": "<port>", // elasticsearch port (can be overriden in 'nodes' field)
    "ssl": "<true or false>", // enables 'https' connections
    "certData": "<elastic certificate>", // optional, use if 'ssl' enabled
    "user": "<credentials: user>",
    "password": "<credentials: password>",
    "index": "<index>" // elasticsearch index (path to data in elastic db)
  }
}
```

### WRITE stages
#### Write to console
```
{
  "id": "<id>",
  "value": {
    "operation": "WRITE",
    "storage": "STDOUT"
  }
}
```
#### Write JDBC
```
{
  "id": "<id>",
  "value": {
    "operation": "WRITE",
    "storage": "<storage>",
    "jdbcUrl": "jdbc:<storage>:<parameters>",
    "table": "<database table name>",
    "user": "<credentials: user>",
    "password": "<credentials: password>",
    "schema": "<database schema>", // optional
    "certData": "<db2 certificate>", // optional
    "writeMode": "<write mode>" // optional, see details below
  }
}
```
see *writeMode* values [here](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html#mode-java.lang.String-) (some values: *errorifexists*(default), *overwrite*, *append*, *ignore*)

#### Write object storage
```
{
  "id": "<id>",
  "value": {
    "operation": "WRITE",
    "storage": "<storage>", // supported values: 'cos', 's3', 'azure-blob-storage', 'google-cloud-storage'
    "accessKey": "<access key>", // credentials
    "secretKey": "<secret key>", // credentials
    "bucket": "<bucket>", // for 'cos', 's3', 'google-cloud-storage'
    "container": "<container>", // for 'azure-blob-storage'
    "path": "<path>", // path in bucket, for 'cos', 's3', 'google-cloud-storage'
    "containerPath": "<path>", // path in container, for 'azure-blob-storage'
    "format": "<format>", // Spark DataFrame format, e.g. 'csv', 'parquet', 'json', etc.
    "endpoint": "<endpoint>", // only for 'cos' and 's3'
    "writeMode": "<write mode>" // optional, see details in 'Write JDBC' section
  }
}
```
#### Write elastic
```
{
  "id": "<id>",
  "value": {
    "operation": "WRITE",
    "storage": "elastic",
    "nodes": "<nodes>", // elasticsearch nodes to connect to (separated by comma)
    "port": "<port>", // elasticsearch port (can be overriden in 'nodes' field)
    "ssl": "<true or false>", // enables 'https' connections
    "certData": "<elastic certificate>", // optional, use if 'ssl' enabled
    "user": "<credentials: user>",
    "password": "<credentials: password>",
    "index": "<index>" // elasticsearch index (path to data in elastic db),
    "writeMode": "<write mode>" // optional, see details in 'Write JDBC' section	
  }
}
```

### Read/Write options
To pass configuration parameters to read and write stages (Object Storage and Elastic supported) option fields should be added in following format: `option.<parameter name>: <parameter value>`  
Example of *cos* *csv* output with changed *delimiter* and enabled *header*:  
```
 {
   "id": "id_cos",
   "value": {
     "operation": "WRITE",
     "storage": "cos",
     "accessKey": "xxx",
     "secretKey": "yyy",
     "bucket": "bucket-name",
     "path": "/path/to/file",
     "format": "csv",
     "option.delimiter": "@",
     "option.header": "true",
     "writeMode": "append",
     "endpoint": "endpoint.example.com"
   }
 }
```

### FUNCTION stages
#### Change data capture
```
{
  "id": "<id>",
  "value": {
    "operation": "CDC",
    "keyColumns": "<column1(,column2(,..))>" // key columns for CDC operation
  }
}
```
#### Filter
```
{
  "id": "<id>",
  "value": {
    "operation": "FILTER",
    "condition": "<where condition>" // part of SQL statement after 'where' keyword
  }
}
```
#### Group by
```
{
  "id": "<id>",
  "value": {
    "operation": "GROUP",
    "groupingColumns": "<column1(,column2(,..))>", // SQL grouping columns
    "groupingCriteria": "opColumn1:operation1(,opColumn2:operation2(,..))", // SQL aggregate columns and operations
    "dropGroupingColumns": "<dropColumns>" // boolean
  }
}
```
#### Join
```
{
  "id": "<id>",
  "value": {
    "operation": "JOIN",
    "joinType": "<type>", // SQL join type: e.g. 'inner', 'left', 'left outer', ...
    "columns": "<column1(,column2(,..))>" // join columns
  }
}
```
#### Remove duplicates
```
{
  "id": "<id>",
  "value": {
    "operation": "REMOVE_DUPLICATES",
    "keyColumns": "<column1(,column2(,..))>", // group columns: records considered duplicated if their values in 'column1', 'column2', ... are equal
    "orderColumns": "<sortColumn1(,sortColumn2(,..))>" // sort columns: resulting record selected useing this sort, other records removed, to define sort following formats also supported: 'column:asc' and 'column:desc'
  }
}
```
#### Transform
```
{
  "id": "<id>",
  "value": {
    "operation": "TRANSFORM",
    "statement": "<SQL select statement>",  // part of SQL statement between 'select' and 'from' keywords
  }
}
```
see available Spark *select* functions [here](https://spark.apache.org/docs/latest/api/sql/index.html)

#### Union
```
{
  "id": "<id>",
  "value": {
    "operation": "UNION",
    "type": "<type>" // supported values: 'distinct', 'all'
  }
}
```

#### AI Text Task - Parse text
```
{
  "id": "<id>",
  "value": {
    "operation": "AI_TEXT_TASK",
    "task": "parseText",
    "llm": "<LLM>", // 'ChatGPT'
    "endpoint": "<endpoint>", // 'https://api.openai.com/v1/chat/completions'
    "model": "<model>", // 'gpt-3.5-turbo'
    "apiKey": "<api key>",
    "sourceColumn": "<source column>",
    "maxTokens": "<max tokens>", // e.g. 1024
    "temperature": "<temperature>", // e.g. 0.7
    "systemMessage": "<system message>", // e.g. 'You are client feedback assistant for the airline company'
    "keepExtraAttributes": "false",
    "attributes": "<attributes>", // attributes in JSON format encoded in base64
    "examples": "<examples>" // examples in JSON format encoded in base64
  }
}
```
other tasks include: Generate Date - generate synthetic data, Transcribe - transcribe speech to text, Generic task - for the rest of use cases

#### Cache
```
{
  "id": "<id>",
  "value": {
    "operation": "CACHE",
    "useDisk" : "<true or false>",
    "useMemory" : "<true or false>",
    "useOffHeap" : "<true or false>",
    "deserialized" : "<true or false>",
    "replication" : "<positive integer>"
  }
}
```
some recommended cache strategies:  

| name \ parameter value | useDisk | useMemory | useOffHeap | deserialized | replication |
| ---------------------- | ------- |  -------- | ---------- | ------------ | ----------- |
| NONE                   | false   | false     | false      | false        | 1           |
| DISK_ONLY              | true    | false     | false      | false        | 1           |
| DISK_ONLY_2            | true    | false     | false      | false        | 2           |
| MEMORY_ONLY            | false   | true      | false      | true         | 1           |
| MEMORY_ONLY_2          | false   | true      | false      | true         | 2           |
| MEMORY_ONLY_SER        | false   | true      | false      | false        | 1           |
| MEMORY_ONLY_SER_2      | false   | true      | false      | false        | 2           |
| MEMORY_AND_DISK        | true    | true      | false      | true         | 1           |
| MEMORY_AND_DISK_2      | true    | true      | false      | true         | 2           |
| MEMORY_AND_DISK_SER    | true    | true      | false      | false        | 1           |
| MEMORY_AND_DISK_SER_2  | true    | true      | false      | false        | 2           |
| OFF_HEAP               | true    | true      | true       | false        | 1           |

### Example
```
{
  "nodes": [
    {
      "id": "gen-table1",
      "value": {
        "operation": "READ",
        "storage": "GENERATOR",
        "alias": "left"
      }
    },
    {
      "id": "dest-table1",
      "value": {
        "operation": "WRITE",
        "storage": "sqlite",
        "jdbcUrl": "jdbc:sqlite:src/main/resources/test.sqlite",
        "table": "p1",
        "user": "user",
        "writeMode": "overwrite",
        "password": "password"
      }
    },
    {
      "id": "source-table1",
      "value": {
        "operation": "READ",
        "storage": "sqlite",
        "jdbcUrl": "jdbc:sqlite:src/main/resources/test.sqlite",
        "table": "p1",
        "user": "user",
        "password": "password"
      }
    },
    {
      "id": "print1",
      "value": {
        "operation": "WRITE",
        "storage": "STDOUT"
      }
    }

  ],
  "edges": [
    {
      "source": "gen-table1",
      "target": "dest-table1"
    },
    {
      "source": "source-table1",
      "target": "print1"
    }

  ]
}
```

### More examples
[Check the examples](./spark-job/src/main/resources)
