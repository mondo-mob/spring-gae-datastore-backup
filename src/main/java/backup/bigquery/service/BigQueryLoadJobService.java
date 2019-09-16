package backup.bigquery.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class BigQueryLoadJobService {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryLoadJobService.class);

    private final BigQuery bigQuery;

    public BigQueryLoadJobService(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    public String startDatastoreBackupLoadJob(String dataset, String kind, String sourceUri) {
        return startLoadJob(dataset, kind, sourceUri, FormatOptions.datastoreBackup(), false);
    }

    public String startLoadJob(String dataset, String kind, String sourceUri, FormatOptions formatOptions, boolean autoDetect) {
        LOG.info("Starting import to bq for {}", sourceUri);
        TableId tableId = TableId.of(dataset, kind);

        LoadJobConfiguration loadJobConfiguration = LoadJobConfiguration.of(tableId, sourceUri)
                .toBuilder()
                .setFormatOptions(formatOptions)
                .setAutodetect(autoDetect)
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                .build();

        Job remoteLoadJob = bigQuery.create(JobInfo.of(loadJobConfiguration));
        try {
            LOG.info("JobId: {}", remoteLoadJob.getJobId());
            LOG.info("SelfLink: {}", remoteLoadJob.getSelfLink());

            remoteLoadJob = remoteLoadJob.waitFor();

            LOG.info(String.format("JobStatus: %s", remoteLoadJob.getStatus()));

            final Table table = bigQuery.getTable(tableId);

            if (!table.exists()) {
                String msg = String.format("Table %s doesn't exist", tableId);
                LOG.warn(msg);
                return msg;
            }
            Long numRows = ((StandardTableDefinition) bigQuery.getTable(tableId).getDefinition()).getNumRows();
            LOG.info(String.format("Number of rows: %s", numRows));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return String.format("BigQuery Load for %s completed", kind);
    }
}
