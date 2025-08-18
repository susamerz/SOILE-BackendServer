package fi.abo.kogni.soile2.projecthandling.apielements;

import org.junit.Test;

import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeResourceManager;
import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.http_server.SoileVerticleTest;
import fi.abo.kogni.soile2.http_server.requestHandling.SOILEUpload;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.projecthandling.utils.ObjectGenerator;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import io.vertx.core.http.MimeMapping;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class APITaskTest extends SoileVerticleTest {

	@Test
	public void testAPIResourceManagement(TestContext context)
	{		
		System.out.println("--------------------  Testing Task Save/Load ----------------------");
		Async testAsync = context.async();
		ElementManager<Task> TaskManager = new ElementManager<Task>(Task::new, APITask::new, mongo_client, vertx);
		DataLakeResourceManager grm = new DataLakeResourceManager(vertx);
		ObjectGenerator.buildAPITask(TaskManager, "FirstTask")
		.onSuccess(apiTask -> {
			// create a new upload.
			String fileName = vertx.fileSystem().createTempFileBlocking("SomeFile", ".ending");			
			SOILEUpload upload = SOILEUpload.create(fileName, "Fun.jpg", MimeMapping.mimeTypeForFilename("Fun.jpg"));
			grm.writeUploadToGit(new GitFile("NewFile",TaskManager.getGitIDForUUID(apiTask.getUUID()),apiTask.getVersion()), upload)
			.onSuccess(newVersion -> {
				Async newVerAsync = context.async();
				// this version should now have NewFile, while the old Version should not.
				vertx.eventBus().request("soile.task.getResourceList", new JsonObject().put("UUID", apiTask.getUUID()).put("version", newVersion))				
				.onSuccess(resourceList-> {
					JsonArray fileList = ((JsonObject) resourceList.body()).getJsonArray(SoileCommUtils.DATAFIELD); 					
					context.assertEquals(2,fileList.size());
					newVerAsync.complete();

				}).onFailure(err -> context.fail(err));
				// this one should NOT have the new File
				Async oldVerAsync = context.async();
				vertx.eventBus().request("soile.task.getResourceList", new JsonObject().put("UUID", apiTask.getUUID()).put("version", apiTask.getVersion()))				
				.onSuccess(resourceList-> {
					JsonArray fileList = ((JsonObject) resourceList.body()).getJsonArray(SoileCommUtils.DATAFIELD); 					
					context.assertEquals(1,fileList.size());
					oldVerAsync.complete();
				}).onFailure(err -> context.fail(err));
				testAsync.complete();
			})
			.onFailure(err -> context.fail(err));						
		})
		.onFailure(err -> context.fail(err));
	}
	
	
	@Test
	public void testAPIStoreLoad(TestContext context)
	{		
		System.out.println("--------------------  Testing Task Save/Load ----------------------");
		Async testAsync = context.async();
		ElementManager<Task> TaskManager = new ElementManager<Task>(Task::new, APITask::new, mongo_client, vertx);
		ObjectGenerator.buildAPITask(TaskManager, "FirstTask")
		.onSuccess(apiTask -> {
			// create a new upload.
			TaskManager.getAPIElementFromDB(apiTask.getUUID(), apiTask.getVersion())
			.onSuccess(retrievedAPITask -> {
				context.assertEquals(apiTask.getGitJson(), retrievedAPITask.getGitJson());
				testAsync.complete();
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}

}

