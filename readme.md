Backup Service (Java 8)
=======================

This app provides functionality to perform a Google Cloud Datastore backup.

It should be deployed as a separate App Engine service called "backup-service". 

### Running locally

    mvn appengine:run

### Deploying

    CLOUDSDK_CORE_PROJECT={PROJECT_ID} mvn appengine:deploy

where `{PROJECT_ID}` is your GCP project id.


Usage
=====

Configure a cron and a task queue in your default service as per the instructions here: https://github.com/3wks/spring-boot-gae/blob/master/README-DATASTORE-BACKUP.md

Note that the cron and task must both be configured with `target: backup-service` to ensure they hit this service. 

You cannot add `cron.xml` or `queue.xml` to this service, it must be added to your default service (ie: your main application).


Backup and Import to BigQuery
=======

As well as regular backups you can configure automatic export and import into BigQuery.
This is a full import/export and any existing data in BigQuery will be deleted for the requested kinds.

### Endpoint

Task endpoint: `/task/bigquery/datastore-export-and-load`

Params:

| Property        | Description           | Required  |
| -------------   |-------------          | -----------|
| name            | name for the datastore export | N |
| dataset         | the target dataset for the import in BigQuery | Y |
| kinds           | the datastore kinds to be exported | Y |
| namespaceId     | optional namespace id to filter by. Only a single namespace is supported. | N |

### Usage

In your default service schedule a cron to trigger an ETL job with target of `backup-service`.

e.g. for XML config

```
<cron>
    <url>/task/bigquery/datastore-export-and-load?name=ExportToBigQuery&dataset=backup_data&kinds=Kind1,Kind2,Kind3</url>
    <description>Datastore export and load to BigQuery</description>
    <target>backup-service</target>
    <schedule>every day 03:00</schedule>
    <timezone>Australia/NSW</timezone>
</cron>
```
or yaml
```
- description: "Datastore export and load to BigQuery"
  url: /task/bigquery/datastore-export-and-load?name=ExportToBigQuery&dataset=backup_data&kinds=Kind1,Kind2,Kind3
  target: backup-service
  schedule: every day 03:00
  timezone: Australia/NSW
```
