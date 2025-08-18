package fi.abo.kogni.soile2.http_server.verticles;

import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeFile;
import fi.abo.kogni.soile2.datamanagement.datalake.ParticipantDataLakeManager;
import fi.abo.kogni.soile2.datamanagement.datalake.ParticipantFileResult;
import fi.abo.kogni.soile2.projecthandling.exceptions.ObjectDoesNotExist;
import fi.abo.kogni.soile2.projecthandling.participant.ParticipantHandler;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.Study;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.impl.StudyHandler;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * Verticle that handles creation of data bundles for Downloads. 
 * @author Thomas Pfau
 *
 */
public class DataBundleGeneratorVerticle extends AbstractVerticle{

	MongoClient client;
	ParticipantHandler partHandler;
	StudyHandler projHandler;
	ParticipantDataLakeManager dlmgr;
	String dataLakeFolder;
	String downloadCollection;
	static final Logger LOGGER = LogManager.getLogger(DataBundleGeneratorVerticle.class);


	/**
	 * Defaul constructor
	 * @param client the {@link MongoClient} for db access
	 * @param projHandler the {@link StudyHandler} for study access
	 * @param partHandler the {@link ParticipantHandler} for participant access
	 */
	public DataBundleGeneratorVerticle(MongoClient client, StudyHandler projHandler, ParticipantHandler partHandler)
	{
		dataLakeFolder = SoileConfigLoader.getServerProperty("soileResultDirectory");
		downloadCollection = SoileConfigLoader.getdbProperty("downloadCollection");
		dlmgr = new ParticipantDataLakeManager(dataLakeFolder, vertx);
		this.client = client;
		this.projHandler = projHandler;
		this.partHandler = partHandler;		
	}

	@Override
	public void start()
	{
		vertx.eventBus().consumer("fi.abo.soile.DLStatus", this::getStatus);
		vertx.eventBus().consumer("fi.abo.soile.DLFiles", this::getDownloadFiles);
		vertx.eventBus().consumer("fi.abo.soile.DLCreate", this::createDownload);		
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void stop(Promise<Void> stopPromise)
	{
		List<Future<Void>> consumerdeactivateFutures = new LinkedList<>();
		consumerdeactivateFutures.add(vertx.eventBus().consumer("fi.abo.soile.DLStatus", this::getStatus).unregister());
		consumerdeactivateFutures.add(vertx.eventBus().consumer("fi.abo.soile.DLFiles", this::getDownloadFiles).unregister());
		consumerdeactivateFutures.add(vertx.eventBus().consumer("fi.abo.soile.DLCreate", this::createDownload).unregister());
		Future.all(consumerdeactivateFutures)
		.onSuccess(success -> {
			LOGGER.debug("Successfully undeployed DataBundleGenerator with id : " + deploymentID());
			stopPromise.complete();
		})
		.onFailure(err -> stopPromise.fail(err));
	}

	/**
	 * Possible options for the status of a download
	 * @author Thomas Pfau
	 *
	 */
	public enum DownloadStatus
	{
		/**
		 * Download is currently being created
		 */
		creating,
		/**
		 * Download files are currently being collected
		 */
		collecting,		
		/**
		 * Download is ready
		 */
		downloadReady,
		/**
		 * There were problems with the download
		 */
		problems,
		/**
		 * Download generation failed
		 */
		failed,
		/**
		 * Download doesn't exist
		 */
		notExistent,
		/**
		 * Download not ready (yet)
		 */
		notReady

	}

	/**
	 * Create a Download for the given Information in the message.
	 * The message needs to have a "requestType" (participants, participant, tasks or task).
	 * participants/tasks need to be JsonArrays, task/participant are strings. 
	 * along with a projectID for which to generate the download.  
	 * @param message the message containing the JsonObject that describes what download to create.
	 */
	public void createDownload(Message<JsonObject> message)
	{
		
		try {
			JsonObject request = message.body();
			switch(request.getString("requestType"))
			{
			case "participants" : buildParticipantsBundle(request.getJsonArray("participants"), request.getString("projectID"))
			.onSuccess(id -> message.reply(id))
			.onFailure(err -> message.fail(0, err.getMessage())); 
			break;
			case "participant" : buildParticipantBundle(request.getString("participant"), request.getString("projectID"))
			.onSuccess(id -> message.reply(id))
			.onFailure(err -> message.fail(0, err.getMessage())); 
			break;

			case "tasks" : buildTasksBundle(request.getJsonArray("tasks"), request.getString("projectID"))
			.onSuccess(id -> message.reply(id))
			.onFailure(err -> message.fail(0, err.getMessage())); 
			break;
			case "task" : buildTaskBundle(request.getString("task"), request.getString("projectID"))
			.onSuccess(id -> message.reply(id))
			.onFailure(err -> message.fail(0, err.getMessage())); 
			break;
			case "all" : buildTasksBundle(null, request.getString("projectID"))
			.onSuccess(id -> message.reply(id))
			.onFailure(err -> message.fail(0, err.getMessage())); 
			break;
			default:
				message.fail(400,"Invalid request");
			}
		}
		catch(ClassCastException e)
		{
			message.fail(400,"Invalid request");
		}
	}

	
	
	/**
	 * Get the status of a download specified in the messages "downloadID" field
	 * @param message the message with the download ID.
	 */
	public void getStatus(Message<JsonObject> message)
	{
		String dlID = message.body().getString("downloadID");
		getDownloadStatus(dlID).onSuccess(status-> {			
			message.reply(status);			
		})
		.onFailure(err -> {
			replyForError(err, message);		
		});
	}

	/**
	 * Get the download files for the indicated downloadID. Returns the status of the download along with the files (if there are any) 
	 * @param message The message containing the download ID.
	 */
	public void getDownloadFiles(Message<JsonObject> message)
	{
		String dlID = message.body().getString("downloadID");
		getDownloadStatus(dlID).onSuccess(status-> {
			getDownloadFilesFromDB(dlID)
			.onSuccess(files -> {
				message.reply(new JsonObject().put("files", files).mergeIn(status));
			})
			.onFailure(err -> replyForError(err, message));

		})
		.onFailure(err -> replyForError(err, message));	
	}

	/**
	 * Create a download for the given participants.  
	 * @param participants The participant IDs of the participants for which to retrieve data.
	 * @param projectID The project ID these participants are in.
	 * @return
	 */
	Future<String> buildParticipantsBundle(JsonArray participants, String projectID)
	{
		return collectParticipantFiles(participants, projectID, false);
	}

	/**
	 * Create a download for the given tasks.  
	 * @param typeID A {@link JsonArray} of task IDs of for which to retrieve data.
	 * @param projectID The project ID these tasks are in.
	 * @return
	 */
	Future<String> buildTasksBundle(JsonArray tasks, String projectID)
	{

		return collectTaskFiles(tasks, projectID, false);
	}

	/**
	 * Create a download for the given task  
	 * @param taskID The task ID of the task for which to retrieve data.
	 * @param projectID The project ID this task is in.
	 * @return
	 */
	Future<String> buildTaskBundle(String taskID, String projectID)
	{

		return collectTaskFiles(new JsonArray().add(taskID), projectID, true);
	}
	/**
	 * Create a download for the given participant.  
	 * @param participant The participant ID of the participant for which to retrieve data.
	 * @param projectID The project ID this participant is in.
	 * @return
	 */
	Future<String> buildParticipantBundle(String participantID, String projectID)
	{
		return collectParticipantFiles(new JsonArray().add(participantID), projectID, true);
	}

	/**
	 * Create a download ID. This is synchronized so that we don't create the same ID twice.
	 * @return
	 */
	private Future<String> createDLID(String sourceProject)
	{	
		return client.save(downloadCollection, new JsonObject().put("status", DownloadStatus.creating.toString())
				.put("timeStamp", System.currentTimeMillis())
				.put("errors", new JsonArray())
				.put("resultFiles", new JsonArray())
				.put("activeDownloads", 0)
				.put("project", sourceProject));		
	}

	/**
	 * Collect the files required for the given Tasks and also build the Output Json file for that request.
	 * @param tasks the tasks for which to obtain the data.
	 * @param projectID the project in which the tasks are.
	 * @param singleTask Whether the request is for a single task output.
	 * @return A {@link Future} of the download ID that is being generated for this request.
	 */
	Future<String> collectTaskFiles( JsonArray tasksToExtract, String projectID, boolean singleTask)
	{
		Promise<String> collectionStartedPromise = Promise.promise();
		if( tasksToExtract == null)
		{
			tasksToExtract = new JsonArray();
		}
		JsonArray tasks = tasksToExtract;
		// Initialize the download
		startDownload(projectID)
		.onSuccess(startedDownload -> {
			String dlID = startedDownload.dlID;
			Study study = startedDownload.projInst;
			if( tasks.isEmpty())
			{				
				for(Object o : study.getTasksInstancesWithNames())
				{
					tasks.add(((JsonObject)o).getString("taskID"));
				}
			}
			study.getParticipants()
			.onSuccess(participants -> {
				LOGGER.debug("The participants of this study are: " + participants.encodePrettily());
				// Collect the data  
				@SuppressWarnings("rawtypes")
				List<Future<Void>> partDataFutures = new LinkedList<>();				
				ConcurrentHashMap<String,List<JsonObject>> taskResults = new ConcurrentHashMap<>();
				for(int i = 0; i < tasks.size(); ++i)
				{
					Promise<Void> addedPromise = Promise.promise();
					partDataFutures.add(addedPromise.future());
					String currentTask = tasks.getString(i);
					partDataFutures.add(partHandler.getTaskDataforParticipants(participants, currentTask, projectID)
							.onSuccess( res -> {
								taskResults.put(currentTask,res);
								addedPromise.complete();
							})
							.onFailure(err -> addedPromise.fail(err)).mapEmpty()
							);

				}
				
				Future.all(partDataFutures)				
				.onSuccess(participantsData -> {
					// at this point the List is filled with all relevant JsonObjects.
					// This is a particpant with _id, resultData
					JsonArray taskInfo = new JsonArray();
					List<JsonObject> resultFiles = new LinkedList<>();
					// clean up and update the data to fit to the output scheme
					for(String task : taskResults.keySet())
					{
						LOGGER.debug("Current Task is: " + task);
						LOGGER.debug("Project Instance is: " + study.toString());
						JsonObject jsonData = new JsonObject();						
						jsonData.put("taskID",task)
						.put("taskName", study.getElement(task).getName());				
						// this will be filled with the actual results for this task.
						JsonArray currentTaskResults = new JsonArray();
						// NOTE: this function heavily modifies the resulting resultData JsonArray					
						resultFiles.addAll(extractFilesandUpdateResultsForTask(taskResults.get(task),currentTaskResults, true));
						
						//jsonData.add(participantData.getJsonArray("resultData"));
						//So, the Data has been updated here as well, we can just use the resultData.						
						// this can potentially be empty, if the participant has no data for this task.
						jsonData.put("resultData", currentTaskResults);
						taskInfo.add(jsonData);
					}					
					checkAndFilterFiles(resultFiles, dlID)
					.onSuccess(filtered -> {
						// Now we have all result files filtered. Lets build the result Json. 
						// TODO: Allow XLS download for projects which can be flattened (i.e. which have no repeating tasks.)
						JsonObject jsonFile;
						if(singleTask)
						{
							// there has to be one.
							if(taskInfo.size() > 0)
							{
								jsonFile = taskInfo.getJsonObject(0);
							}
							else
							{
								failDownload(new Exception("No data found for task"), dlID, "Did not find data for the task");
								return;
							}
						}
						else 
						{
							jsonFile = new JsonObject()
									.put("project", new JsonObject()
											.put("id", study.getID())
											.put("name", study.getName()))
									.put("taskResults", taskInfo);
						}
						LOGGER.debug(jsonFile.encodePrettily());
						createJsonAndFinishDownload(dlID, projectID, jsonFile, resultFiles);	
					})
					.onFailure(err -> failDownload(err, dlID, "Unable to filter files for download " + dlID));																
				})
				.onFailure(err -> failDownload(err, dlID, "Couldn't obtain participant data for download " + dlID));
			})
			.onFailure(err -> failDownload(err, dlID,"Couldn't obtain participants for download " + dlID));
			collectionStartedPromise.complete(dlID);			
		}).onFailure(err -> collectionStartedPromise.fail(err));

		return collectionStartedPromise.future();
	}			

	/**
	 * Collect the files required for the given Participants and also build the Output Json file for that request.
	 * @param participants the participants for which to obtain the data.
	 * @param projectID the project in which the tasks are.
	 * @param singleTask whether the request is for a single task output.
	 * @return A {@link Future} of the download ID that is being generated for this request.
	 */
	Future<String> collectParticipantFiles(JsonArray participants, String projectID, boolean singleParticipant)
	{

		Promise<String> collectionStartedPromise = Promise.promise();
		startDownload(projectID)
		.onSuccess(startedDownload -> {
			String dlID = startedDownload.dlID;
			Study study = startedDownload.projInst;
			LOGGER.debug("Obtaining data for: " + participants.encode());

			partHandler.getParticipantData(study, participants)
			.onSuccess( participantData -> {
				LOGGER.debug("Participant Data Obtained");
				// the format of this list is: 
				/*
				 * _id : partipantID
				 * resultData : participant result data array
				 * steps : step order for the participant
				 * finished : whether the participant is finished.
				 */
				// at this point the List is filled with all relevant JsonObjects.
				// This is a particpant with _id, resultData
				JsonArray participantResults = new JsonArray();
				List<JsonObject> resultFiles = new LinkedList<>();
				LOGGER.debug("Number of Participant Information: " + participantData.size());

				LOGGER.debug("Participant Data is: \n" + new JsonArray(participantData).encodePrettily());
				for(JsonObject partData: participantData)
				{
					JsonArray resultElements = partData.getJsonArray("resultData"); 

					// NOTE: this function modifies the provided resultData JsonArray, in particular the files field. 					
					resultFiles.addAll(extractFilesandUpdateParticipantData(resultElements,partData.getString("_id")));
					partData.remove("steps");
					partData.remove("finished");
					partData.put("participantID", partData.getValue("_id")).remove("_id");
					participantResults.add(partData);
				}	

				checkAndFilterFiles(resultFiles, dlID)
				.onSuccess(filtered -> {
					LOGGER.debug("Files filtered");
					// Now we have all result files filtered. Lets build the result Json. 
					// TODO: Allow XLS download for projects which can be flattened (i.e. which have no repeating tasks.)
					JsonObject jsonFile;
					if(singleParticipant)
					{
						// there has to be one.
						if(participantResults.size() > 0)
						{
							jsonFile = participantResults.getJsonObject(0);
						}
						else
						{
							failDownload(new Exception("No data found for participant"), dlID, "Did not find data for the participant");
							return;
						}
					}
					else 
					{
						jsonFile = new JsonObject()
								.put("project", new JsonObject()
										.put("id", study.getID())
										.put("name", study.getName()))
								.put("participantResults", participantResults);
					}
					createJsonAndFinishDownload(dlID, projectID, jsonFile, resultFiles);					

				})
				.onFailure(err -> failDownload(err, dlID, "Unable to check and Filter files for download " + dlID));		
			})

			.onFailure(err -> failDownload(err, dlID, "Couldn't obtain participant data for download " + dlID));
			collectionStartedPromise.complete(dlID);							
		})		
		.onFailure(err -> collectionStartedPromise.fail(err));
		return collectionStartedPromise.future();

	}

	/**
	 * Initiate a download. 
	 * @param projectID
	 * @return
	 */
	private Future<Download> startDownload(String projectID)
	{
		Promise<Download> collectionStartedPromise = Promise.promise();
		createDLID(projectID)
		.onSuccess(dlID -> {
			projHandler.loadUpToDateStudy(projectID)
			.onSuccess(study ->
			{		
				client.updateCollection(downloadCollection, new JsonObject().put("_id",dlID), statusUpdate(DownloadStatus.collecting))
				.onSuccess(colStart -> {
					collectionStartedPromise.complete(new Download(study,dlID));				
				}).onFailure(err -> collectionStartedPromise.fail(err));
			})				
			.onFailure(err -> collectionStartedPromise.fail(err));
		})		
		.onFailure(err -> collectionStartedPromise.fail(err));

		return collectionStartedPromise.future();
	}

	/**
	 * Extract the datalake files indicated in the FileResults of the resultsData.
	 * @param typeID the current task that is being handled
	 * @param participantID the participant ID of the participant currently bing handled
	 * @param resultData One object of the resultData Json Array according to the Participant specifications
	 * @param extractedFileNames the List of DataLakeFiles that is required.
	 */

	private List<JsonObject> extractFilesandUpdateParticipantData(JsonArray resultData, String participantID) 
	{		
		List<JsonObject> files = new LinkedList<>();
		for(int i = 0; i < resultData.size(); i++)
		{	
			JsonObject current = resultData.getJsonObject(i);
			files.addAll(extractFilesandUpdateResults(current, participantID,false));

		}
		return files;
	}	

	private void createJsonAndFinishDownload(String dlID, String projectID, JsonObject jsonFile, List<JsonObject> resultFiles)
	{		
		vertx.fileSystem().createTempFile(SoileConfigLoader.getServerProperty("soileResultDirectory"), projectID, ".json", "rw-rw----")
		.onSuccess(fileName -> {
			vertx.fileSystem().writeFile(fileName, Buffer.buffer(jsonFile.encodePrettily()))
			.onSuccess(written -> 
			{
				LOGGER.debug("Json File written");
				// Lets collect the Data for the resultFiles									
				JsonObject fileUpdates = statusUpdate(DownloadStatus.downloadReady);
				JsonObject setObject = fileUpdates.getJsonObject("$set");
				setObject.put("jsonDataLocation", new JsonObject().put("nameInZip", "data.json")
						.put("filename", Path.of(fileName).getFileName().toString())//.replace(SoileConfigLoader.getServerProperty("soileResultDirectory"), "")) // the name IN the the folder.
						.put("mimeType", "application/json"))
				.put("resultFiles",new JsonArray(resultFiles));
				LOGGER.debug(new JsonArray(resultFiles).encodePrettily());
				// this needs no more success Information.											
				client.updateCollection(downloadCollection, new JsonObject().put("_id", dlID), fileUpdates)
				.onFailure(err -> failDownload(err, dlID, "Couldn't update final file information for download " + dlID))
				.onSuccess(res -> {
					LOGGER.debug("Status updated in database; Download Ready");
				});

			})
			.onFailure(err -> failDownload(err, dlID, "Couldn't write temporary json file for download " + dlID));																			
		})
		.onFailure(err -> failDownload(err, dlID, "Couldn't create temporary json file for download " + dlID));	
	}

	/**
	 * This function assumes to have been provided with the resultData {@link JsonArray} from a participant.
	 * @param resultData
	 * @param participantID
	 * @return
	 */
	private List<JsonObject> extractFilesandUpdateResultsForTask(Collection<JsonObject> resultData, JsonArray taskData, boolean removeTask) 
	{
		List<JsonObject> files = new LinkedList<>();

		for(JsonObject current : resultData)
		{	
			LOGGER.debug(current.encodePrettily());

			// this is the data for one participant which can be multiple results for a single task, so we need to loop over this (and update stuff).
			// each object in the collectio is all data for one participant.
			JsonArray currentParticipantData = current.getJsonArray("participantData", new  JsonArray());
			for(int i = 0 ; i < currentParticipantData.size(); ++i)
			{
				JsonObject currentResults = currentParticipantData.getJsonObject(i);
				files.addAll(extractFilesandUpdateResults(currentResults, currentResults.getString("participantID"),removeTask));
				taskData.add(currentResults);
			}			

		}
		return files;
	}	
	/**
	 * Extract the files from the resultData and updates the results such that it contains a files field that contains the information for file data retrieval.
	 * @param resultData a JsonObject from a participants result Data. With at least the following fields:
	 * 					task: 
                			type: string
                			description: "Has to follow the format t[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]"
              			step:
                			type: integer
                			description: "Indicator of which step this was for the user in the project"
				      	dbData:
				        	type: object
					      	required: 
					        	- name
					        	- value
					      	properties:
					        	name: 
					          		type: string
					          		description: the name of the output, the format is "[0-9A-Za-z]+"
					          		example: "smoker"
					        	value:
					          		type: string or number
 			          				description: The value of an output (within outputData this can only be a number)
				      	fileData:
				        	type: array
					        items:
					         	type: object
					         	required: 
					           		- targetid
					           		- fileformat
					           		- filename
					         	properties: 
					           		fileformat:
					             		type: string
					            		description: The (mime) format of the stored file.
					           		filename: 
					             		type: string
					             		description: the supposed file name of a supplied file.
					          		targetid:
					               		type: string
					               		description: The ID of the file. This is the value returned by the /project/{id}/uploadData endpoint.
	 * 						  
	 * @param participantID the ID of the current Participant, necessary to obtain the correct location for the file in question
	 * @param removeTask whether to remove the task field (if this prodces task results, the task id is in a parent object)
	 * @return a List of JsonObjects that represent the datalakeFiles indicated by these results.
	 */
	private List<JsonObject> extractFilesandUpdateResults(JsonObject resultData, String participantID, boolean removeTask) 
	{
		List<JsonObject> files = new LinkedList<>();		
		LOGGER.debug(resultData.encodePrettily());

		String taskID = resultData.getString("task");
		int step =  resultData.getInteger("step");
		JsonArray fileData = resultData.getJsonArray("fileData");
		JsonArray filesData = new JsonArray();
		for(int fileEntry = 0; fileEntry < fileData.size(); fileEntry++)
		{
			JsonObject fileResult = fileData.getJsonObject(fileEntry);
			ParticipantFileResult res = new ParticipantFileResult(fileResult.getString("targetid"),
					fileResult.getString("filename"),
					fileResult.getString("fileformat"),
					step,
					taskID,
					participantID);				
			String pathInZip = taskID + "/" + participantID + "/" + step + "/" + res.getFile(dataLakeFolder).getOriginalFileName();
			JsonObject fileInfo = new JsonObject().put("originalFileName", res.getFile(dataLakeFolder).getOriginalFileName())
					.put("path", pathInZip)
					.put("mimeType", fileResult.getString("fileformat"));
			filesData.add(fileInfo);
			DataLakeFile currentFile = res.getFile(dataLakeFolder);
			currentFile.setOriginalFileName(pathInZip);
			LOGGER.debug(currentFile.toJson().encodePrettily());
			files.add(currentFile.toJson());
			if(removeTask)
			{
				resultData.remove("task");
			}
		}
		resultData.put("files", filesData);		
		resultData.remove("fileData");
		return files;
	}

	/**
	 * Get the download status for a given download ID.
	 * @param dlID the download ID
	 * @return a {@link Future} with the status.
	 */
	Future<JsonObject> getDownloadStatus(String dlID)
	{
		Promise<JsonObject> statusPromise = Promise.promise();
		client.findOne(downloadCollection, new JsonObject().put("_id",dlID), new JsonObject().put("_id",0).put("status",1).put("errors", 1))
		.onSuccess(result -> {
			if(result == null)
			{
				statusPromise.fail(new ObjectDoesNotExist(dlID));				
			}
			else
			{
				LOGGER.debug("Status for dl " + dlID + " is: " + result.getString("status"));
				if(result.containsKey("errors") && result.getJsonArray("errors").size() == 0)
				{
					result.remove("errors");
				}
				statusPromise.complete(result);
			}
		})
		.onFailure(err -> statusPromise.fail(err));
		return statusPromise.future();
	}

	/**
	 * Get the download problems for a given download ID.
	 * @param dlID the download ID
	 * @return a {@link Future} with the status.
	 */
	Future<JsonArray> getDownloadIssues(String dlID)
	{
		Promise<JsonArray> statusPromise = Promise.promise();
		client.findOne(downloadCollection, new JsonObject().put("_id",dlID), new JsonObject().put("errors",1))
		.onSuccess(result -> {
			if(result == null)
			{
				statusPromise.fail(new ObjectDoesNotExist(dlID));				
			}
			else
			{
				LOGGER.debug("Status for dl " + dlID + " is: " + result.getString("status"));
				statusPromise.complete(result.getJsonArray("errors"));
			}
		})
		.onFailure(err -> statusPromise.fail(err));
		return statusPromise.future();
	}

	/**
	 * Get the Files for a given download from the database.
	 * @param dlID the download for which the files are requested.
	 * @return
	 */
	Future<JsonArray> getDownloadFilesFromDB(String dlID)
	{
		Promise<JsonArray> dlPromise = Promise.promise();

		client.findOne(downloadCollection, new JsonObject().put("_id",dlID), new JsonObject().put("status",1).put("resultFiles",1).put("jsonDataLocation", 1))
		.onSuccess(result -> {
			if(result == null)
			{				
				dlPromise.fail(new ObjectDoesNotExist(dlID));				
			}
			else
			{
				if(result.getString("status").equals(DownloadStatus.downloadReady.toString())) 
				{

					JsonArray results = result.getJsonArray("resultFiles");
					JsonObject jsonDataFile = result.getJsonObject("jsonDataLocation");
					DataLakeFile f = new DataLakeFile(dataLakeFolder, jsonDataFile.getString("filename"), jsonDataFile.getString("nameInZip"), jsonDataFile.getString("mimeType"));					
					results.add(f.toJson());
					dlPromise.complete(results);
				}
				else
				{
					dlPromise.fail(new DownloadNotReadyException(dlID));	
				}
			}
		})
		.onFailure(err -> dlPromise.fail(err));
		return dlPromise.future();
	}

	Future<Void> checkAndFilterFiles(List<JsonObject> fileIndicators, String dlID)
	{
		Promise<Void> checkAndFilterPromise = Promise.promise();
		checkForFileProblems(fileIndicators)
		.onSuccess(existIndicators -> {
			filterFilesAndStatus(fileIndicators, existIndicators, dlID)
			.onSuccess(success -> checkAndFilterPromise.complete())
			.onFailure(err -> checkAndFilterPromise.fail(err));
		})
		.onFailure(err -> checkAndFilterPromise.fail(err));
		return checkAndFilterPromise.future();
	}
	/**
	 * Filter the files given the existIndicators and indicate errors if there are any missing files.
	 * @param files The files to filter
	 * @param existsIndicator a boolean indicator for each file, whether it exists.
	 * @param dlID the dlID currently handled
	 */
	private Future<Void> filterFilesAndStatus(List<JsonObject> files, List<Boolean> existsIndicator, String dlID)
	{		
		List<String> missingFileNames = new LinkedList<String>();
		List<JsonObject> toRemove = new LinkedList<>();
		for(int i = 0; i < existsIndicator.size(); ++i)
		{
			if(!existsIndicator.get(i)) {
				missingFileNames.add(files.get(i).getString("absolutePath").replace(dataLakeFolder, ""));
				toRemove.add(files.get(i));
			}
		}
		if(toRemove.size() > 0)
		{						
			files.removeAll(toRemove);
			return downloadIssues("Files missing: " + String.join("", missingFileNames), dlID);
		}				
		else
		{
			return Future.succeededFuture();
		}
	}	

	/**
	 * Check The given list of file indicators (more precise the "absolutePath" fields of the objects on whether they exist
	 * @param filesIndicators the "files" to check.
	 * @return A Future of a List of booleans where each true item indicates that the file at that position exists.
	 */
	public Future<List<Boolean>> checkForFileProblems(List<JsonObject> filesIndicators)
	{
		Promise<List<Boolean>> errorPromise = Promise.promise();
		@SuppressWarnings("rawtypes")
		List<Future<Void>> filesExistFutures = new LinkedList<Future<Void>>();

		for(JsonObject file : filesIndicators)
		{
			filesExistFutures.add(checkFileExist(file));									
		}
		CompositeFuture filesExist =  Future.join(filesExistFutures);	
		// There are failing and succeeding futures in this.
		filesExist.onComplete(finishedCheck -> {
			List<Boolean> fileExistIndicator = new LinkedList<>();
			for(int i = 0; i < filesExistFutures.size(); ++i)				
			{
				fileExistIndicator.add(filesExist.succeeded(i));
			}
			errorPromise.complete(fileExistIndicator);
		});		
		return errorPromise.future();

	}

	/**
	 * Check whether the file indicated by the indicator exists.
	 * @param fileIndicator
	 * @return A Future that suceeds if and only if the file could be checked AND exists.
	 */
	Future<Void> checkFileExist(JsonObject fileIndicator)
	{
		Promise<Void> fileExists = Promise.promise();						
		try {

			vertx.fileSystem().exists(new DataLakeFile(fileIndicator).getAbsolutePath())
			.onSuccess(res -> {
				if(res)
				{
					fileExists.complete();
				}
				else
				{
					LOGGER.error("Couldn't find file: " + fileIndicator.encode());
					fileExists.fail("File does not exist");
				}
			})
			.onFailure(err -> fileExists.fail(err));
		}
		catch(Exception e)
		{
			fileExists.fail(e);
		}
		return fileExists.future();
	}

	private Future<Void> failDownload(Throwable err, String dlID, String problem)
	{
		LOGGER.error("Download failed due to: " + problem);
		LOGGER.error(err);
		return client.updateCollection(downloadCollection, 
				new JsonObject().put("_id",dlID), 				
				statusUpdate(DownloadStatus.failed).put("$push", new JsonObject().put("errors",err.getMessage())))
				.onFailure(err2 ->
				{										
					LOGGER.error("Couldn't update download DB for item " + dlID);
					LOGGER.error("Originally failed due to :");
					LOGGER.error(err);
					LOGGER.error("DB update failed due to :");
					LOGGER.error(err2);
				}).mapEmpty();				
	}	

	private Future<Void> downloadIssues(String err, String dlID)
	{
		return client.updateCollection(downloadCollection, 
				new JsonObject().put("_id",dlID), 
				new JsonObject().put("$push", new JsonObject().put("errors",err)))
				.mapEmpty();		
	}	


	private JsonObject statusUpdate(DownloadStatus newStatus)
	{
		return new JsonObject().put("$set", new JsonObject().put("status", newStatus.toString()));
	}
	@SuppressWarnings("rawtypes")
	private void replyForError(Throwable err, Message request)
	{
		if(err instanceof ObjectDoesNotExist)
		{
			request.reply(new JsonObject().put("status", DownloadStatus.notExistent.toString()));	
		}
		else if(err instanceof DownloadNotReadyException) 
		{
			request.reply(new JsonObject().put("status", DownloadStatus.notReady.toString()));
		}
		else
		{
			request.reply(new JsonObject().put("status", DownloadStatus.problems.toString()).put("message", err.getMessage()));
		}		
	}

	private class Download{

		public String dlID;
		public Study projInst;
		public Download(Study p, String dlID)
		{
			this.dlID = dlID;
			this.projInst = p;
		}
	}

	private class DownloadNotReadyException extends Exception
	{
		private static final long serialVersionUID = 1L;

		public DownloadNotReadyException(String id) {
			super(id + " not ready");
		}
	}
}
