package fi.abo.kogni.soile2.datamanagement.cleanup;

import java.io.File;
import java.nio.file.Path;

import org.junit.Test;

import fi.abo.kogni.soile2.http_server.SoileWebTest;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.projecthandling.apielements.APITask;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import fi.abo.kogni.soile2.utils.WebObjectCreator;
import io.vertx.core.http.MimeMapping;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class GitCleanerTest extends SoileWebTest{

	@Test
	public void testHistory(TestContext context)
	{	
		System.out.println("--------------------  Testing history obtained  ----------------------");    

		ElementManager<Task> taskManager = new ElementManager<Task>(Task::new, APITask::new, mongo_client,vertx);
		String TestDataFolder = WebObjectCreator.class.getClassLoader().getResource("FileTestData").getPath();
		Async setupAsync = context.async();
		GitCleaner cleaner = new GitCleaner(mongo_client, vertx, taskManager);
		createUserAndAuthedSession("TestUser", "testPassword", Roles.Researcher)
		.onSuccess(session -> {
			WebObjectCreator.createTask(session, "PsychoPyExample")
			.onSuccess( taskData -> {
				String taskID = taskData.getString("UUID");
				postTaskRessource(session, taskID , taskData.getString("version"), "Video.mp4",
						new File(Path.of(TestDataFolder, "Video.mp4").toString()) , MimeMapping.mimeTypeForFilename("Video.mp4") )										
				.onSuccess( newVersion -> {		
					taskManager.getVersionListForElement(taskData.getString("UUID"))
					.onSuccess(versionList -> {
						JsonArray tagableVersions = new JsonArray();
						JsonArray inaccessibleVersions = new JsonArray();
						for(Object o : versionList)
						{
							JsonObject current = (JsonObject) o;
							if(current.containsKey("canbetagged") && current.getBoolean("canbetagged") || current.containsKey("tag"))
							{
								tagableVersions.add(current.getString("version"));
							}
							else
							{
								inaccessibleVersions.add(current.getString("version"));
							}									
						}	
						context.assertTrue(inaccessibleVersions.contains(newVersion));
						context.assertTrue(tagableVersions.contains(taskData.getString("version")));
						Async retrieveHistoryAsync = context.async();
						
						cleaner.retrieveTagHistories(taskID, tagableVersions)
						.onSuccess(tagHistoryVersions -> {
							for(Object o : tagHistoryVersions)
							{
								inaccessibleVersions.remove(o);
							}
							context.assertEquals(1,inaccessibleVersions.size());
							context.assertTrue(inaccessibleVersions.contains(newVersion));
							retrieveHistoryAsync.complete();
						})
						.onFailure(err -> context.fail(err));
						setupAsync.complete();
					})
					.onFailure(err -> context.fail(err));
				})
				.onFailure(err -> context.fail(err));

			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}
	
	@Test
	public void testRecentNotDeleted(TestContext context)
	{	
		System.out.println("--------------------  Testing that recent changes don't get deleted  ----------------------");    

		ElementManager<Task> taskManager = new ElementManager<Task>(Task::new, APITask::new, mongo_client,vertx);
		String TestDataFolder = WebObjectCreator.class.getClassLoader().getResource("FileTestData").getPath();
		Async setupAsync = context.async();
		GitCleaner cleaner = new GitCleaner(mongo_client, vertx, taskManager);
		GitCleaner immediateCleaner = new GitCleaner(mongo_client, vertx, taskManager,0L);
		createUserAndAuthedSession("TestUser", "testPassword", Roles.Researcher)
		.onSuccess(session -> {
			WebObjectCreator.createTask(session, "PsychoPyExample")
			.onSuccess( taskData -> {
				String taskID = taskData.getString("UUID");
				postTaskRessource(session, taskID , taskData.getString("version"), "Video.mp4",
						new File(Path.of(TestDataFolder, "Video.mp4").toString()) , MimeMapping.mimeTypeForFilename("Video.mp4") )										
				.onSuccess( newVersion -> {		
					taskManager.getVersionListForElement(taskData.getString("UUID"))
					.onSuccess(versionList -> {
						Async cleanupAsync = context.async();
						cleaner.cleanTask(taskID)
						.onSuccess(cleaned -> {
							
							taskManager.getVersionListForElement(taskData.getString("UUID"))
							.onSuccess(cleanedVersionList -> {	
								context.assertEquals(cleanedVersionList, versionList);
								immediateCleaner.cleanTask(taskID)
								.onSuccess(cleaned2 -> {
									taskManager.getVersionListForElement(taskData.getString("UUID"))
									.onSuccess(cleanedVersionList2 -> {	
										context.assertNotEquals(cleanedVersionList2, versionList);
										context.assertEquals(versionList.size()-1, cleanedVersionList2.size());
										cleanupAsync.complete();
									})
									.onFailure(err -> context.fail(err));
								})
								.onFailure(err -> context.fail(err));
							})
							.onFailure(err -> context.fail(err));
						})
						.onFailure(err -> context.fail(err));
																		
												
						setupAsync.complete();
					})
					.onFailure(err -> context.fail(err));
				})
				.onFailure(err -> context.fail(err));

			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}
	
	
	@Test
	public void testDetails(TestContext context)
	{	
		System.out.println("--------------------  Testing individual parts of Task cleanup  ----------------------");    

		ElementManager<Task> taskManager = new ElementManager<Task>(Task::new, APITask::new, mongo_client,vertx);
		String TestDataFolder = WebObjectCreator.class.getClassLoader().getResource("FileTestData").getPath();
		Async setupAsync = context.async();
		GitCleaner cleaner = new GitCleaner(mongo_client, vertx, taskManager, 0L);
		createUserAndAuthedSession("TestUser", "testPassword", Roles.Researcher)
		.onSuccess(session -> {
			WebObjectCreator.createTask(session, "PsychoPyExample")
			.onSuccess( taskData -> {
				String taskID = taskData.getString("UUID");
				postTaskRessource(session, taskID , taskData.getString("version"), "Video.mp4",
						new File(Path.of(TestDataFolder, "Video.mp4").toString()) , MimeMapping.mimeTypeForFilename("Video.mp4") )										
				.onSuccess( newVersion -> {		
					taskManager.getVersionListForElement(taskData.getString("UUID"))
					.onSuccess(versionList -> {						
						JsonArray tagableVersions = new JsonArray();
						JsonArray inaccessibleVersions = new JsonArray();
						for(Object o : versionList)
						{
							JsonObject current = (JsonObject) o;
							if(current.containsKey("canbetagged") && current.getBoolean("canbetagged") || current.containsKey("tag"))
							{
								tagableVersions.add(current.getString("version"));
							}
							else
							{
								inaccessibleVersions.add(current.getString("version"));
							}									
						}													
						Async getInaccessibleAsync = context.async();
						cleaner.retrieveTagHistories(taskID,tagableVersions)
						.onSuccess(accessibleVersions -> {
							// need to collect all File IDs for all accessibleVersions
							// and all for non accessible versions.
							// then delete those only in inaccessible versions, and remove the inaccessibleversions.
							for(Object o : accessibleVersions)
							{
								inaccessibleVersions.remove(o);
							}
							context.assertEquals(1, inaccessibleVersions.size());
							cleaner.getInaccessibleFileIDs(taskID, accessibleVersions, inaccessibleVersions)
							.onSuccess(inaccessible -> {
								context.assertEquals(1, inaccessible.size());
								cleaner.cleanTask(taskID)
								.onSuccess(cleaned -> {
									taskManager.getVersionListForElement(taskData.getString("UUID"))
									.onSuccess(cleanedVersions -> {
																				
										context.assertEquals(versionList.size()-1, cleanedVersions.size());
										// cleaned Versions does not contain the removed version
										for(Object o : cleanedVersions)
										{
											JsonObject current = (JsonObject) o;
											context.assertNotEquals(inaccessibleVersions.getValue(0), current.getString("version") );																				
										}
										// check, that files were deleted
										File fileToCheck = new File(SoileConfigLoader.getServerProperty("soileGitDataLakeFolder") + File.separator + Task.typeID + taskID + File.separator + inaccessible.getString(0));
										vertx.fileSystem().exists(fileToCheck.getAbsolutePath())
										.onSuccess(exists -> {
											context.assertFalse(exists);
											getInaccessibleAsync.complete();	
										})
										.onFailure(err -> context.fail(err));										
									})
									.onFailure(err -> context.fail(err));
								})
								.onFailure(err -> context.fail(err));
								context.assertEquals(1, inaccessible.size());
							})
							.onFailure(err -> context.fail(err));
							
						})
						.onFailure(err -> context.fail(err));
						
												
						setupAsync.complete();
					})
					.onFailure(err -> context.fail(err));
				})
				.onFailure(err -> context.fail(err));

			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}
}
