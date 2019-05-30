package backup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.contrib.gae.datastore.config.CloudDatastoreBackupAutoConfiguration;

@Import({
		CloudDatastoreBackupAutoConfiguration.class,
})
@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}