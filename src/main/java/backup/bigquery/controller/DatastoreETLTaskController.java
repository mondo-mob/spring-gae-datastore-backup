package backup.bigquery.controller;

import backup.bigquery.service.DatastoreETLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.contrib.gae.datastore.entity.BackupOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RestController
public class DatastoreETLTaskController {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreETLTaskController.class);

    private static final String TASK_DATASTORE_EXPORT = "/task/bigquery/datastore-export-and-load";
    private static final String TASK_DATASTORE_EXPORT_PARAM_NAME = "name";
    private static final String TASK_DATASTORE_EXPORT_PARAM_DATASET = "dataset";
    private static final String TASK_DATASTORE_EXPORT_PARAM_KINDS = "kinds";
    private static final String TASK_DATASTORE_EXPORT_PARAM_NAMESPACE_ID = "namespaceId";

    public static final String TASK_DATASTORE_EXPORT_CHECK = "/task/bigquery/datastore-export-check";
    public static final String TASK_DATASTORE_EXPORT_CHECK_PARAM_DATASET = "dataset";
    public static final String TASK_DATASTORE_EXPORT_CHECK_PARAM_KINDS = "kinds";
    public static final String TASK_DATASTORE_EXPORT_CHECK_PARAM_BACKUP_OPERATION_ID = "backupOperationId";

    public static final String TASK_BIGQUERY_LOAD = "/task/bigquery/datastore-load-kind";
    public static final String TASK_BIGQUERY_LOAD_PARAM_DATASET = "dataset";
    public static final String TASK_BIGQUERY_LOAD_PARAM_KIND = "kind";
    public static final String TASK_BIGQUERY_LOAD_PARAM_GCS_OBJECT_PATH = "gcsObjectPath";

    private final DatastoreETLService datastoreETLService;

    public DatastoreETLTaskController(DatastoreETLService datastoreETLService) {
        this.datastoreETLService = datastoreETLService;
    }

    @GetMapping(TASK_DATASTORE_EXPORT)
    public String startDatastoreExport(
            @RequestParam(value = TASK_DATASTORE_EXPORT_PARAM_NAME, defaultValue = "Datastore Export for BigQuery") final String name,
            @RequestParam(value = TASK_DATASTORE_EXPORT_PARAM_DATASET) final String dataset,
            @RequestParam(value = TASK_DATASTORE_EXPORT_PARAM_KINDS) final List<String> kinds,
            @RequestParam(value = TASK_DATASTORE_EXPORT_PARAM_NAMESPACE_ID, required = false) final String namespaceId) {
        LOG.info("Datastore ETL into BigQuery: Start datastore export");

        List<String> kindList = splitParams(kinds);

        BackupOperation backupOperation = datastoreETLService.startDatastoreExport(dataset, name, kindList, namespaceId);

        LOG.info("Backup operation {} started", backupOperation.getName());
        return String.format("Backup operation %s started", backupOperation.getName());
    }

    @GetMapping(TASK_DATASTORE_EXPORT_CHECK)
    public String startDatastoreExportCheck(
            @RequestParam(value = TASK_DATASTORE_EXPORT_CHECK_PARAM_BACKUP_OPERATION_ID) String backupOperationId,
            @RequestParam(value = TASK_DATASTORE_EXPORT_CHECK_PARAM_DATASET) String dataset,
            @RequestParam(value = TASK_DATASTORE_EXPORT_CHECK_PARAM_KINDS) String kinds) {
        LOG.info("Datastore ETL into BigQuery: Check datastore export status");

        List<String> kindList = Arrays.asList(kinds.split(","));

        BackupOperation backupOperation = datastoreETLService.getBackupOperation(backupOperationId);

        if (!backupOperation.isDone()) {
            datastoreETLService.enqueueDatastoreExportCheck(backupOperationId, dataset, kindList);
            return String.format("Backup operation %s hasn't finished yet, will check again later", backupOperationId);
        }

        LOG.info("Datastore ETL into BigQuery: Datastore export complete");
        datastoreETLService.startBigQueryLoad(backupOperation, dataset, kindList);
        return "BigQuery load queued...";
    }

    @GetMapping(TASK_BIGQUERY_LOAD)
    public String startBigQueryLoadJob(
            @RequestParam(value = TASK_BIGQUERY_LOAD_PARAM_GCS_OBJECT_PATH) String gcsObjectPath,
            @RequestParam(value = TASK_BIGQUERY_LOAD_PARAM_DATASET) String dataset,
            @RequestParam(value = TASK_BIGQUERY_LOAD_PARAM_KIND) String kind) {
        LOG.info("Datastore ETL into BigQuery: Load kind {} into BigQuery dataset {}", kind, dataset);
        return datastoreETLService.startBigQueryLoadForKind(dataset, kind, gcsObjectPath);
    }

    // Support params being sent as comma delimited list and/or as separate HTTP params
    // i.e. url?kinds=Entity1,Entity2 or url?kinds=Entity1&kinds=Entity2
    private List<String> splitParams(List<String> strings) {
        return strings == null ? new ArrayList<>() : strings.stream()
                .flatMap(Pattern.compile(",")::splitAsStream)
                .collect(Collectors.toList());
    }
}
