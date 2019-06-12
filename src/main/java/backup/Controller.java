package backup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

	private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);

	public Controller() {
	}

	@GetMapping(value = "/", produces = MediaType.TEXT_PLAIN_VALUE)
	public String index() {
		LOGGER.info("Backup service");
		return "backup-service - performs Google Cloud Datastore backups";
	}

}
