package backup.bigquery.controller;


import backup.bigquery.service.DatastoreETLService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.contrib.gae.datastore.entity.BackupOperation;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
public class DatastoreETLTaskControllerTest {

	@Mock
	private DatastoreETLService datastoreETLService;

	@InjectMocks
	private DatastoreETLTaskController backupController;

	private MockMvc mvc;

	@Before
	public void setup() {
		mvc = MockMvcBuilders
			.standaloneSetup(backupController)
			.alwaysDo(print())
			.build();
	}

	private void mockStartExport(String dataset, String name, String namespaceId) {
		BackupOperation backupOperation = new BackupOperation().setName(name);
		when(datastoreETLService.startDatastoreExport(eq(dataset), eq(name), anyList(), eq(namespaceId))).thenReturn(backupOperation);
	}

	@Test
	public void startDatastoreExport_WillStartNewExportAndLoad_withoutNamespaceId() throws Exception {
		mockStartExport("target_dataset", "Datastore ETL", null);

		mvc.perform(get("/task/bigquery/datastore-export-and-load")
			.param("name", "Datastore ETL")
			.param("dataset", "target_dataset")
			.param("kinds", "Entity1,Entity2")
		)
			.andExpect(status().isOk());

		verify(datastoreETLService).startDatastoreExport("target_dataset", "Datastore ETL", Arrays.asList("Entity1", "Entity2"), null);
	}

	@Test
	public void startDatastoreExport_WillStartNewExportAndLoad_withNamespaceId() throws Exception {
		mockStartExport("target_dataset", "Datastore ETL", "Namespace1");

		mvc.perform(get("/task/bigquery/datastore-export-and-load")
			.param("name", "Datastore ETL")
			.param("dataset", "target_dataset")
			.param("kinds", "Entity1,Entity2")
			.param("namespaceId", "Namespace1")
		)
			.andExpect(status().isOk());

		verify(datastoreETLService).startDatastoreExport("target_dataset", "Datastore ETL", Arrays.asList("Entity1", "Entity2"), "Namespace1");
	}

	@Test
	public void startDatastoreExport_willAllowKindsToBeSentAsCommaSeparatedList() throws Exception {
		mockStartExport("target_dataset", "Datastore ETL", null);

		mvc.perform(get("/task/bigquery/datastore-export-and-load")
			.param("name", "Datastore ETL")
			.param("dataset", "target_dataset")
			.param("kinds", "E1,E2,E3")
			.param("kinds", "E4,E5"))
			.andExpect(status().isOk());

		verify(datastoreETLService).startDatastoreExport("target_dataset", "Datastore ETL", Arrays.asList("E1", "E2", "E3", "E4", "E5"), null);
	}

	@Test
	public void startDatastoreExport_willFailWithoutKinds() throws Exception {
		mvc.perform(get("/task/bigquery/datastore-export-and-load")
			.param("name", "Backup")
			.param("dataset", "target_dataset")
		)
			.andExpect(status().isBadRequest())
			.andExpect(status().reason("Required List parameter 'kinds' is not present"));
	}

	@Test
	public void startDatastoreExport_willFailWithoutDataset() throws Exception {
		mvc.perform(get("/task/bigquery/datastore-export-and-load")
			.param("name", "Backup")
			.param("kinds", "Backup")
		)
			.andExpect(status().isBadRequest())
			.andExpect(status().reason("Required String parameter 'dataset' is not present"));
	}
}
