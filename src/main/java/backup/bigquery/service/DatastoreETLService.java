package backup.bigquery.service;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.contrib.gae.datastore.entity.BackupOperation;
import org.springframework.contrib.gae.datastore.repository.BackupOperationRepository;
import org.springframework.contrib.gae.datastore.service.DatastoreBackupService;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

import static backup.bigquery.controller.DatastoreETLTaskController.TASK_BIGQUERY_LOAD;
import static backup.bigquery.controller.DatastoreETLTaskController.TASK_BIGQUERY_LOAD_PARAM_DATASET;
import static backup.bigquery.controller.DatastoreETLTaskController.TASK_BIGQUERY_LOAD_PARAM_GCS_OBJECT_PATH;
import static backup.bigquery.controller.DatastoreETLTaskController.TASK_BIGQUERY_LOAD_PARAM_KIND;
import static backup.bigquery.controller.DatastoreETLTaskController.TASK_DATASTORE_EXPORT_CHECK;
import static backup.bigquery.controller.DatastoreETLTaskController.TASK_DATASTORE_EXPORT_CHECK_PARAM_BACKUP_OPERATION_ID;
import static backup.bigquery.controller.DatastoreETLTaskController.TASK_DATASTORE_EXPORT_CHECK_PARAM_DATASET;
import static backup.bigquery.controller.DatastoreETLTaskController.TASK_DATASTORE_EXPORT_CHECK_PARAM_KINDS;
import static com.googlecode.objectify.ObjectifyService.ofy;

@RestController
public class DatastoreETLService {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreETLService.class);

    private static final int COUNTDOWN_TO_CHECK_BACKUP_STATUS = 6 * 60 * 1000;

    private final Queue backupQueue;
    private final DatastoreBackupService datastoreBackupService;
    private final BigQueryLoadJobService bigQueryLoadJobService;
    private final BackupOperationRepository backupOperationRepository;

    public DatastoreETLService(Queue backupQueue,
                               DatastoreBackupService datastoreBackupService,
                               BigQueryLoadJobService bigQueryLoadJobService,
                               BackupOperationRepository backupOperationRepository) {
        this.backupQueue = backupQueue;
        this.datastoreBackupService = datastoreBackupService;
        this.bigQueryLoadJobService = bigQueryLoadJobService;
        this.backupOperationRepository = backupOperationRepository;
    }

    public BackupOperation startDatastoreExport(String dataset, String name, List<String> kinds, String namespaceId) {
        List<String> namespaceIdList = namespaceId == null ? Collections.emptyList() : Collections.singletonList(namespaceId);

        BackupOperation backupOperation = datastoreBackupService.startBackup(name, kinds, namespaceIdList);

        enqueueDatastoreExportCheck(backupOperation.getName(), dataset, kinds);

        return backupOperation;
    }

    public BackupOperation getBackupOperation(String backupOperationId) {
        return backupOperationRepository.getById(backupOperationId);
    }

    public void startBigQueryLoad(
            BackupOperation backupOperation,
            String dataset,
            List<String> kinds) {

        kinds.forEach(kind -> {
            String gcsObjectPath = String.format("%s/all_namespaces/kind_%s/all_namespaces_kind_%s.export_metadata", backupOperation.getOutputUrlPrefix(), kind, kind);

            LOG.info("Queue importing kind {} from GCS: {}", kind, gcsObjectPath);
            enqueueBigQueryLoadJob(gcsObjectPath, dataset, kind);
        });
    }

    public String startBigQueryLoadForKind(String dataset, String kind, String gcsObjectPath) {
        LOG.info("Start importing kind {} into BigQuery dataset {}", kind, dataset);
        return bigQueryLoadJobService.startDatastoreBackupLoadJob(dataset, kind, gcsObjectPath);
    }

    public void enqueueDatastoreExportCheck(String backupOperationId, String dataset, List<String> kinds) {
        String kindsString = String.join(",", kinds);
        TaskOptions taskOptions = TaskOptions.Builder
                .withUrl(TASK_DATASTORE_EXPORT_CHECK)
                .param(TASK_DATASTORE_EXPORT_CHECK_PARAM_BACKUP_OPERATION_ID, backupOperationId)
                .param(TASK_DATASTORE_EXPORT_CHECK_PARAM_DATASET, dataset)
                .param(TASK_DATASTORE_EXPORT_CHECK_PARAM_KINDS, kindsString)
                .countdownMillis(COUNTDOWN_TO_CHECK_BACKUP_STATUS)
                .method(TaskOptions.Method.GET);

        addToQueue(taskOptions);
    }

    private void enqueueBigQueryLoadJob(String gcsObjectPath, String dataset, String kind) {
        TaskOptions taskOptions = TaskOptions.Builder
                .withUrl(TASK_BIGQUERY_LOAD)
                .param(TASK_BIGQUERY_LOAD_PARAM_GCS_OBJECT_PATH, gcsObjectPath)
                .param(TASK_BIGQUERY_LOAD_PARAM_DATASET, dataset)
                .param(TASK_BIGQUERY_LOAD_PARAM_KIND, kind)
                .method(TaskOptions.Method.GET);

        addToQueue(taskOptions);
    }

    private void addToQueue(TaskOptions taskOptions) {
        backupQueue.add(ofy().getTransaction(), taskOptions);
    }
}
