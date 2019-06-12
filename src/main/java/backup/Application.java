package backup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.contrib.gae.datastore.config.CloudDatastoreBackupAutoConfiguration;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.provisioning.UserDetailsManager;

@Import({
		CloudDatastoreBackupAutoConfiguration.class,
})
@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	@ConditionalOnMissingBean
	public UserDetailsManager getUserDetailsManager() {

		// Note that this is not used by the app, but including it prevents warning logs on startup
		return new InMemoryUserDetailsManager();
	}

}