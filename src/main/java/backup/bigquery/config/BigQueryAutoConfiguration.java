package backup.bigquery.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.contrib.gae.datastore.config.ConfigurationException;
import org.springframework.contrib.gae.objectify.config.ObjectifyConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;


@Configuration
public class BigQueryAutoConfiguration implements ObjectifyConfigurer {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryAutoConfiguration.class);
    private static final List<String> BIGQUERY_SCOPES = singletonList("https://www.googleapis.com/auth/bigquery");

    @Bean
    @ConditionalOnMissingBean(name = "bigQueryCredential")
    public GoogleCredentials bigQueryCredential() {
        try {
            LOG.info("Using application default credential");
            return GoogleCredentials
                    .getApplicationDefault()
                    .createScoped(BIGQUERY_SCOPES);
        } catch (IOException e) {
            throw new ConfigurationException(e, "BigQuery client configuration failed: %s", e.getMessage());
        }
    }

    @Bean
    public BigQuery bigQueryClient(Environment environment, GoogleCredentials bigQueryCredential) {
        if (environment.acceptsProfiles(Profiles.of("local"))) {
            LOG.info("Not configuring BigQuery client for local environment");
            return null;
        }

        return BigQueryOptions.newBuilder()
                .setCredentials(bigQueryCredential)
                .build()
                .getService();
    }
}

