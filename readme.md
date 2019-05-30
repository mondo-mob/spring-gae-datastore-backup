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

