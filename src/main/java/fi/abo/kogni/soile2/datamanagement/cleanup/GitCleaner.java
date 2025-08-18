package fi.abo.kogni.soile2.datamanagement.cleanup;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeResourceManager;
import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * This class is a means to clean up unneeded files from the datalake. 
 * Execution is quite expensive as it will need to check all (potentially) 
 * accessible versions for all Tasks (Experiments and Projects do not store 
 * additional information.    
 * @author Thomas Pfau
 *
 */
public class GitCleaner {

	private static Logger LOGGER = LogManager.getLogger(GitCleaner.class.getName());
	
	MongoClient client;
	Vertx vertx;
	ElementManager<Task> taskManager;
	DataLakeResourceManager drm;
	long maxAge;
	
	
	/**
	 * Like the normal constructor but assumes a maxAge of 12 hours.
	 * @param client the mongoclient used for this cleaner (communication with db)
	 * @param vertx the vertx instance the cleaner is running for
	 * @param taskManager the {@link ElementManager} to use 
	 */
	public GitCleaner(MongoClient client, Vertx vertx, ElementManager<Task> taskManager) {
		// default maxAge is 12 hours
		this(client,vertx,taskManager, 1000*60*60*12);
	}
	/**
	 * Create a GitCleaner with a specified maxAge. The maxAge indicates how old a version can be to still be
	 * considered reachable for deletion purposes (i.e. we assume this could be under active work)
	 * @param client the mongoclient used for this cleaner (communication with db)
	 * @param vertx the vertx instance the cleaner is running for
	 * @param taskManager the {@link ElementManager} to use
	 * @param maxAge a maxage  before reloading data
	 */
	public GitCleaner(MongoClient client, Vertx vertx, ElementManager<Task> taskManager, long maxAge) {
		super();
		this.client = client;
		this.vertx = vertx;
		this.taskManager = taskManager;
		this.drm = new DataLakeResourceManager(vertx);
		this.maxAge = maxAge;
	}
	
	
	/**
	 * Clean up a task. All Files and versions that are not accessible (via tags) or have been accessible will be removed from the task. 
	 * @param taskID the task to clean up.
	 * @return a Future indicating if the 
	 */
	public Future<Void> cleanTask(String taskID)
	{
		Promise<Void> cleanUpPromise = Promise.promise();
		taskManager.getVersionListForElement(taskID)
		.onSuccess(versionList -> {
			JsonArray reachableVersions = new JsonArray();
			JsonArray inaccessibleVersions = new JsonArray();
			Long currentTime = new Date().getTime();			
			for(Object o : versionList)
			{
				JsonObject current = (JsonObject) o;				
				Long versionAge = currentTime - current.getLong("date");				
				
				if(current.containsKey("canbetagged") && current.getBoolean("canbetagged") 
						|| current.containsKey("tag") 
						|| maxAge > versionAge ) 
				{					
					reachableVersions.add(current.getString("version"));
				}
				else
				{
					inaccessibleVersions.add(current.getString("version"));
				}				
			}
			retrieveTagHistories(taskID,reachableVersions)
			.onSuccess(accessibleVersions -> {
				// need to collect all File IDs for all accessibleVersions
				// and all for non accessible versions.
				// then delete those only in inaccessible versions, and remove the inaccessibleversions.
				for(Object o : accessibleVersions)
				{
					inaccessibleVersions.remove(o);
				}
				getInaccessibleFileIDs(taskID, accessibleVersions, inaccessibleVersions)
				.onSuccess(filesToDelete -> {
					// now, we need to delete all these filesand remove the inaccessible versions. 
					cleanFilesAndDB(taskID, inaccessibleVersions, filesToDelete)
					.onSuccess(done -> cleanUpPromise.complete())
					.onFailure(err -> cleanUpPromise.fail(err));
				})
				.onFailure(err -> cleanUpPromise.fail(err));
			})
			.onFailure(err -> cleanUpPromise.fail(err));
		})
		.onFailure(err -> cleanUpPromise.fail(err));

		return cleanUpPromise.future();
	}	
	
	Future<JsonArray> getInaccessibleFileIDs(String taskID, JsonArray acessibleVersions, JsonArray inaccessibleVersions)	
	{
		Promise<JsonArray> inaccessibleFilePromise = Promise.promise();
		getFileIDsForVersions(taskID, inaccessibleVersions)
		.onSuccess(inaccessibleIDs -> {
			getFileIDsForVersions(taskID, acessibleVersions)
			.onSuccess(accessibleIDs -> {
				JsonArray inaccessibleFileIDS = new JsonArray();				
				
				for(Object o : inaccessibleIDs)
				{					
					if(!accessibleIDs.contains(o))
					{
						inaccessibleFileIDS.add(o);
					}
				}
				inaccessibleFilePromise.complete(inaccessibleFileIDS);
			})
			.onFailure(err -> inaccessibleFilePromise.fail(err));
		})
		.onFailure(err -> inaccessibleFilePromise.fail(err));
				
		return inaccessibleFilePromise.future();
	}
	
	
	Future<JsonArray> getFileIDsForVersions(String taskID, JsonArray taskVersions)
	{
		// this is expensive, but probably the only way to not accidentially remove files.
		Promise<JsonArray> dataLakefileIDs = Promise.promise();																			
		LinkedList<Future<JsonArray>> fileIDs = new LinkedList<>();
		fileIDs.add(Future.succeededFuture(new JsonArray()));
		List<Future<JsonArray>> fileIDRequests = new LinkedList<>();
		// if we don't have anything to clean, we need to make sure, that there is at least one future..
		fileIDRequests.add(Future.succeededFuture());
		for(int i = 0; i < taskVersions.size(); i++)
		{
			String currentVersion = taskVersions.getString(i);
			Future<JsonArray> CurrentFuture = fileIDs.getLast()
					.compose(currentFileIDs -> {					
						return getFileIDsForVersion(taskID, currentVersion)
								.compose(newFileIDs -> 
								{
									return  Future.succeededFuture(currentFileIDs.addAll(newFileIDs));									
								});						

					});			
			fileIDs.add(CurrentFuture);
			fileIDRequests.add(CurrentFuture);										
		}
		 
		Future.all(fileIDRequests).onSuccess(done -> {
			dataLakefileIDs.complete(fileIDs.getLast().result());
		})
		.onFailure(err -> dataLakefileIDs.fail(err));

		return dataLakefileIDs.future();

	}
	Future<JsonArray> getFileIDsForVersion(String taskID, String taskVersion)
	{
		return vertx.eventBus().request("soile.task.getResourceList",new JsonObject().put("UUID", taskID).put("version", taskVersion))
				.compose(response -> {

					Promise<JsonArray> dataLakefileIDs = Promise.promise();
					JsonArray files = ((JsonObject) response.body()).getJsonArray(SoileCommUtils.DATAFIELD);														
					JsonArray fileNames = getFileNamesForResources(files, "");			
					LinkedList<Future<JsonArray>> fileIDs = new LinkedList<>();
					fileIDs.add(Future.succeededFuture(new JsonArray()));
					List<Future<JsonArray>> fileIDRequests = new LinkedList<>();			
					// if we don't have anything to clean, we need to make sure, that there is at least one future..
					fileIDRequests.add(Future.succeededFuture());
					for(int i = 0; i < fileNames.size(); i++)
					{
						String currentFileName = fileNames.getString(i);
						Future<JsonArray> CurrentFuture = fileIDs.getLast()
								.compose(currentFileIDs -> {					

									return vertx.eventBus().request("soile.git.getGitResourceContentsAsJson", new GitFile(currentFileName, Task.typeID + taskID, taskVersion).toJson())
											.compose(filecontent -> {												
												JsonObject content = (JsonObject) filecontent.body();
												return Future.succeededFuture(currentFileIDs.add(content.getString("targetFile")));
											});


								});			
						fileIDs.add(CurrentFuture);
						fileIDRequests.add(CurrentFuture);										
					}
					Future.all(fileIDRequests).onSuccess(done -> {
						dataLakefileIDs.complete(fileIDs.getLast().result());
					})
					.onFailure(err -> dataLakefileIDs.fail(err));

					return dataLakefileIDs.future();
				});
	}
	/**
	 * The resources are in the format returned by getResourceList. i.e. 
	 * [{label: "fileOrFolderName", children [{as this, only present if this is a folder}] } ]
	 * @param resources
	 * @param baseFolder
	 * @return
	 */
	JsonArray getFileNamesForResources(JsonArray resources, String baseFolder)
	{
		JsonArray fileNames = new JsonArray();
		for(int i = 0; i < resources.size(); i++)
		{			
			JsonObject currentElement = resources.getJsonObject(i);
			if(currentElement.containsKey("children"))
			{
				String newFolder =  baseFolder + currentElement.getString("label") + "/" ;  
				fileNames.addAll(getFileNamesForResources(currentElement.getJsonArray("children"), newFolder));
			}
			else
			{
				fileNames.add(baseFolder + currentElement.getString("label"));
			}
		}
		return fileNames;
	}

	private JsonArray addMissing(JsonArray a, JsonArray b)
	{		
		JsonArray newArray = new JsonArray();		
		newArray.addAll(a);
		for(Object o : b)
		{
			if(!a.contains(o))
			{
				a.add(o);
			}
		}
		return newArray;
	}

	/**
	 * Retrieve all commits that are part of the histories of certain versions.
	 * @param taskUUID the taskUUID to check
	 * @param tagableVersions the versions for which to obtain history versions
	 * @return
	 */
	Future<JsonArray> retrieveTagHistories(String taskUUID, JsonArray tagableVersions)
	{
		Promise<JsonArray> retrievableTags = Promise.promise();

		LinkedList<Future<JsonArray>> histories = new LinkedList<>();
		histories.add(Future.succeededFuture(new JsonArray()));
		List<Future<JsonArray>> historyRequests = new LinkedList<>();
		// if we don't have anything to clean, we need to make sure, that there is at least one future..
		historyRequests.add(Future.succeededFuture());
		for(int i = 0; i < tagableVersions.size(); i++)
		{
			int currentElement = i;
			Future<JsonArray> CurrentFuture = histories.getLast()					
					.compose(currentVersions -> {
						if(currentVersions.contains(tagableVersions.getString(currentElement )))
						{
							return Future.succeededFuture(currentVersions);
						}
						else
						{
							// add  only those 
							return vertx.eventBus().request("soile.task.getHistory", new JsonObject().put("UUID", taskUUID).put("version", tagableVersions.getString(currentElement)))
									.compose(response -> {
										JsonArray versionList = ((JsonObject) response.body()).getJsonArray(SoileCommUtils.DATAFIELD);														
										return Future.succeededFuture(addMissing(versionList,currentVersions));
									});
						}
					});			
			histories.add(CurrentFuture);
			historyRequests.add(CurrentFuture);								
		}
		Future.all(historyRequests).onSuccess(done -> {
			retrievableTags.complete(histories.getLast().result());
		})
		.onFailure(err -> retrievableTags.fail(err));

		return retrievableTags.future();
	}
	
	/**
	 * Clean files and the database removing unreachable data. 
	 * @param taskID
	 * @param inaccessibleVersions
	 * @param filesToDelete
	 * @return
	 */
	Future<Void> cleanFilesAndDB(String taskID, JsonArray inaccessibleVersions, JsonArray filesToDelete)
	{
		Promise<Void> cleanupDone = Promise.promise();
		JsonObject versionPull = new JsonObject()
									.put("$pull", new JsonObject()
											.put("versions", new JsonObject()
													.put("version", new JsonObject()
															.put("$in", inaccessibleVersions))));		
		// remove the unreachable versions from the list 
		client.findOneAndUpdate(SoileConfigLoader.getdbProperty("taskCollection"), 
								new JsonObject().put("_id", taskID), 
								versionPull)
		.onSuccess(originalElement -> {
				
			List<Future<Void>> FileDeletionFutures = new LinkedList<>();
			// if we don't have anything to clean, we need to make sure, that there is at least one future..
			FileDeletionFutures.add(Future.succeededFuture());
			for(int i = 0; i < filesToDelete.size(); ++i)
			{
				FileDeletionFutures.add(drm.deleteDataLakeFile(taskID, filesToDelete.getString(i)));
			}
			Future.all(FileDeletionFutures)
			.onSuccess(filesDeleted -> {
				LOGGER.debug("Cleanup done");
				cleanupDone.complete();
			})
			.onFailure(err -> {
				JsonObject versionFixUpdate = new JsonObject().put("$set", new JsonObject().put("versions", originalElement.getJsonArray("versions"))); 
				client.findOneAndUpdate(SoileConfigLoader.getdbProperty("taskCollection"), 
								new JsonObject().put("_id", taskID), versionFixUpdate)
				.onSuccess(done -> {
					LOGGER.error("Error deleting files, But able to recover original element");
					cleanupDone.fail(err);
				})
				.onFailure(err2 -> {
					LOGGER.error("Error deleting files, and unable to recover original element. Original data was: \n" + originalElement.encodePrettily());
					cleanupDone.fail(err2);
				});
			});
		})
		.onFailure(err -> cleanupDone.fail(err));
		return cleanupDone.future();
	}
}
