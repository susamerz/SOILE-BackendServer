package fi.abo.kogni.soile2.projecthandling.participant;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.utils.DirtyDataRetriever;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.DuplicateUserEntryInDBException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.UserDoesNotExistException;
import fi.abo.kogni.soile2.projecthandling.participant.impl.DBParticipantFactory;
import fi.abo.kogni.soile2.projecthandling.participant.impl.TokenParticipantFactory;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.Study;
import fi.abo.kogni.soile2.utils.MongoAggregationHandler;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.prng.VertxContextPRNG;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.UpdateOptions;

/**
 * This class provides functionalities to interact with the Participant db, creating, saving and deleting participants.
 * @author Thomas Pfau
 *
 */
public class ParticipantManager implements DirtyDataRetriever<String, Participant> {

	MongoClient client;
	// should go to the constructor.	
	private ParticipantFactory factory;
	private ParticipantFactory tokenfactory;
	private HashMap<String, Long> dirtyTimeStamps; 
	//TODO: needs constructor.
	private String participantCollection = SoileConfigLoader.getdbProperty("participantCollection");
	/**
	 * Logger
	 */
	public static final Logger LOGGER = LogManager.getLogger(ParticipantManager.class);

	/**
	 * Default constructor
	 * @param client the {@link MongoClient} for db access
	 */
	public ParticipantManager(MongoClient client)
	{
		this.client  = client;
		this.factory = new DBParticipantFactory(this);
		this.tokenfactory = new TokenParticipantFactory(this);
		dirtyTimeStamps = new HashMap<>();
	}

	@Override
	public Future<Participant> getElement(String key) {
		Promise<Participant> participantPromise = Promise.<Participant>promise();
		JsonObject query = new JsonObject().put("_id", key);
		client.find(participantCollection, query).onSuccess(res ->
		{
			//There should be only one
			if(res.size() != 1)
			{
				if(res.size() == 0)
				{
					participantPromise.fail(new NoSuchElementException());
				}
				else
				{
					participantPromise.fail(new DuplicateUserEntryInDBException(key));
				}
			}
			else
			{
				JsonObject partObject = res.get(0);
				Participant p;
				if(partObject.getString("token") != null)
				{
					p = tokenfactory.createParticipant(partObject);
				}
				else
				{
					p = factory.createParticipant(partObject);
				}
				dirtyTimeStamps.put(key,System.currentTimeMillis());
				participantPromise.complete(p);				
			}
		}).onFailure(err -> 
		{
			participantPromise.fail(err);
		});			
		return participantPromise.future();
	}

	@Override
	public void getElement(String key, Handler<AsyncResult<Participant>> handler) {
		handler.handle(getElement(key));
	}

	@Override
	public Future<Participant> getElementIfDirty(String key) {
		Promise<Participant> dirtyPromise = Promise.promise();
		client.findOne(participantCollection,  new JsonObject().put("_id", key), new JsonObject().put("modifiedStamp",1))
		.onSuccess(res -> {
			if(dirtyTimeStamps.get(key) >= res.getLong("modifiedStamp"))
			{
				dirtyPromise.complete(null);
			}
			else
			{
				getElement(key).onSuccess(part ->
				{
					dirtyPromise.complete(part);
				})
				.onFailure(err -> dirtyPromise.fail(err));
			}
		})
		.onFailure(err -> dirtyPromise.fail(err));
		return dirtyPromise.future();
	}

	@Override
	public void getElementIfDirty(String key, Handler<AsyncResult<Participant>> handler) {
		handler.handle(getElementIfDirty(key));		
	}


	/**
	 * Create a new Participant with empty information and retrieve a new ID from the 
	 * database.
	 * @param study the study to create a participant in.
	 * @param signupToken a signUpToken (optional, can be null or empty)
	 * @param tokenParticipant whether the created Participant is a token participant
	 * @return A {@link Future} of the created {@link Participant}
	 */
	public Future<Participant> createParticipant(Study study, String signupToken, boolean tokenParticipant)
	{
		Promise<Participant> participantPromise = Promise.<Participant>promise();
		JsonObject defaultParticipant = getDefaultParticipantInfo(study.getID());
		if(signupToken != null && !signupToken.equals(""))			
		{
			defaultParticipant.put("signupToken", signupToken);
		}
		client.save(participantCollection,defaultParticipant).onSuccess(res ->
		{
			defaultParticipant.put("_id", res);
						
			if(tokenParticipant)
			{				
				VertxContextPRNG rng = VertxContextPRNG.current();		
				// the token contains the project ID, to retrieve it from the token, if needed.				
				String Token = rng.nextString(35) + "$"+ study.getID();
				defaultParticipant.put("token", res + Token );
				createTokenParticipant(study, defaultParticipant)
				.onSuccess(participant -> {
					participantPromise.complete(participant);
				})
				.onFailure(err -> participantPromise.fail(err));
			}
			else {				
				createParticipant(study, defaultParticipant)
				.onSuccess(participant -> {
					participantPromise.complete(participant);
				})
				.onFailure(err -> participantPromise.fail(err));
			}

		}).onFailure(err -> {
			participantPromise.fail(err);
		});
		return participantPromise.future();
	}

	/**
	 * Create a new Participant with empty information and retrieve a new ID from the 
	 * database.
	 * @param currentStudy the study to create a participant for
	 * @param defaultParticipant data for a default participant
	 * @return A Future of the created {@link Participant}
	 */
	private Future<Participant> createParticipant(Study currentstudy, JsonObject defaultParticipant)
	{	
		Promise<Participant> participantPromise = Promise.<Participant>promise();		
		Participant part = factory.createParticipant(defaultParticipant);
		currentstudy.addParticipant(part).onSuccess( Void -> 
		{
			participantPromise.complete(part);
		})
		.onFailure(err -> participantPromise.fail(err));				
		return participantPromise.future();
	}


	/**
	 * Create a new Token Participant with empty information and retrieve a new ID from the 
	 * database.
	 * @param currentStudy the study to create a participant for
	 * @param defaultParticipant data for a default participant
	 * @return A Future of the created {@link Participant}
	 */
	private Future<Participant> createTokenParticipant(Study currentStudy, JsonObject defaultParticipant)
	{	
		Promise<Participant> participantPromise = Promise.<Participant>promise();
		// only set a token, if a token was actually used.
		// this ensures, that the generated token is UNIQUE for the participant collection and still cannot be easily guessed.			
		client.findOneAndUpdate(participantCollection,new JsonObject().put("_id", defaultParticipant.getValue("_id")), new JsonObject().put("$set",defaultParticipant))
		.onSuccess(stored ->
		{										
			Participant part = tokenfactory.createParticipant(defaultParticipant);
			currentStudy.addParticipant(part).onSuccess( Void -> 
			{
				participantPromise.complete(part);
			})
			.onFailure(err -> participantPromise.fail(err));	
		})
		.onFailure(err -> participantPromise.fail(err));						

		return participantPromise.future();
	}


	/**
	 * Delete a participant and all associated data from the database
	 * @param id the id of the participant
	 * @param handler a handle to handle the (mongo client result of the deletion operation)
	 */
	public void deleteParticipant(String id, Handler<AsyncResult<JsonObject>> handler)
	{		
		handler.handle(client.findOneAndDelete(participantCollection, new JsonObject().put("_id", id)));
	}

	/**
	 * Delete a participant from the participant database. 
	 * This does NOT remove other data associated with the participant.
	 * @param id the ID of the participant.
	 * @return A Future that indicates the project the deleted participant was in.
	 */
	public Future<String> deleteParticipant(String id)
	{					
		return client.findOneAndDelete(participantCollection, new JsonObject().put("_id", id)).map(res -> {return res.getString("project");});
	}


//	/**
//	 * Get The files stored for a specific participant. 
//	 * @param resultJson a {@link JsonObject} that contains at least a "resultData" array from a participant along with the "_id" field.
//	 * @return 
//	 */
//	public Set<ParticipantFileResult> getFilesFromResults(JsonObject resultJson)
//	{
//
//		Set<ParticipantFileResult> results = new HashSet<>();
//		JsonArray resultData = resultJson.getJsonArray("resultData", new JsonArray());
//		for(int i = 0; i < resultData.size(); i++)
//		{
//			JsonObject taskResults = resultData.getJsonObject(i);
//			int step = taskResults.getInteger("step");
//			JsonArray fileResults = taskResults.getJsonArray("fileData", new JsonArray());
//			for(int j = 0; i < fileResults.size(); j++)
//			{
//				JsonObject fileResult = fileResults.getJsonObject(j);				
//				results.add(new ParticipantFileResult(fileResult.getString("targetid"),
//						fileResult.getString("filename"),											   
//						fileResult.getString("fileformat"),
//						step,
//						taskResults.getString("task"), 
//						resultJson.getString("_id")));
//			}
//		}
//		return results;
//	}
//
//	/**
//	 * Get a list of dbResults for the given participants. 
//	 * @param participantIDs
//	 * @return A fture of the list of JsonResults.
//	 */
//	public Future<List<JsonObject>> getDataBaseResultsForParticipants(List<String> participantIDs, boolean finished)
//	{
//		JsonObject matchObj = new JsonObject().put("_id", new JsonArray(participantIDs));
//		if(finished)
//		{
//			matchObj.put("finished", true);
//		}
//		FindOptions options = new FindOptions();
//		options.setFields(new JsonObject().put("_id", 1).put("resultData", new JsonObject().put("task", 1).put("dbData", 1)));
//		return client.findWithOptions(participantCollection, matchObj,options);
//	}
//
//	/**
//	 * Get the tasks from a result object that contain files
//	 * @param resultJson a {@link JsonObject} that contains at least a "resultData" array from a participant.  
//	 * @return 
//	 */
//	public Set<String> getTaskWithFilesFromResults(JsonObject resultJson)
//	{
//
//		Set<String> results = new HashSet<>();
//		JsonArray resultData = resultJson.getJsonArray("resultData", new JsonArray());
//		for(int i = 0; i < resultData.size(); i++)
//		{
//			JsonObject taskResults = resultData.getJsonObject(i);
//			JsonArray fileResults = taskResults.getJsonArray("fileData", new JsonArray());
//			if(fileResults.size() > 0)
//			{
//				results.add(taskResults.getString("task"));
//			}
//		}
//		return results;
//	}
//
//
//	/**
//	 * Get the results stored for a specific participant. 
//	 * @param id the ID of the participant.
//	 * @return 
//	 */
//	public Future<JsonObject> getParticipantResults(String participantID)
//	{		
//		return client.findOne(participantCollection, new JsonObject().put("_id", participantID), new JsonObject().put("resultData", 1).put("_id", 1));
//	}


	/**
	 * Default schema of a Participant.
	 * @param projectID the project id for which to get default data
	 * @return an empty participant information {@link JsonObject}
	 */
	public static JsonObject getDefaultParticipantInfo(String projectID)
	{
		return new JsonObject().put("position","")
				.put("project",projectID)
				.put("finished", false)							   
				.put("outputData", new JsonArray())
				.put("modifiedStamp", System.currentTimeMillis())
				.put("resultData", new JsonArray());

	}

	/**
	 * Save the given participant.
	 * @param p the {@link Participant} to save
	 * @return a {@link Future} of id of the {@link Participant}
	 */
	public Future<String> save(Participant p)
	{
		//TODO: Possibly we need to fix this in some way to avoid concurrent handling 
		// of participants, but 
		Promise<String> savePromise = Promise.<String>promise();
		p.toJson()
		.onSuccess(update -> {
			update.put("modifiedStamp", System.currentTimeMillis());
			if(p.hasToken())
			{
				update.put("token", p.getToken());
			}
			LOGGER.debug("Updating participant with update: " + update.encodePrettily());
			update.remove("_id");
			JsonObject dataUpdate = new JsonObject().put("$set", update);			
			client.updateCollectionWithOptions(participantCollection,new JsonObject().put("_id", p.getID()),dataUpdate, new UpdateOptions().setUpsert(true))
			.onSuccess(res -> {
				savePromise.complete(p.getID());
			})
			.onFailure(err -> savePromise.fail(err));
		})
		.onFailure(err -> savePromise.fail(err));				
		return savePromise.future();
	}


	/**
	 * Update the results for the given task. 
	 * @param taskID the Task to be updated
	 * @param results the results { an element of the resultData array with (or without) the task ID}
	 * @param p the participant
	 * @param step the step to update
	 * @return a {@link Future} indicating success of the operation
	 */
	public Future<Void> updateResults(Participant p, int step, String taskID, JsonObject results)
	{
		// we will pull whatever data was set in the results for this element.
		// and put in what we just got.
		JsonObject pullUpdate = new JsonObject().put("$pull", new JsonObject()
				.put("resultData", new JsonObject()
						.put("$and", new JsonArray()
								.add( new JsonObject()
										.put("task", taskID)
										.put("step", step)))));
		JsonObject pushUpdate = new JsonObject().put("$push", new JsonObject()
				.put("resultData", new JsonObject().put("task", taskID).put("step", step).mergeIn(results)));
		JsonObject setUpdate = new JsonObject().put("$set", new JsonObject()
				.put("modifiedStamp", System.currentTimeMillis()));		
		JsonObject itemQuery = new JsonObject().put("_id", p.getID());
		List<BulkOperation> pullAndPut = new LinkedList<>();
		BulkOperation pullOp = BulkOperation.createUpdate(itemQuery, pullUpdate);
		BulkOperation pushOp = BulkOperation.createUpdate(itemQuery, pushUpdate);
		BulkOperation setOp = BulkOperation.createUpdate(itemQuery, setUpdate);
		pullAndPut.add(pullOp);
		pullAndPut.add(pushOp);
		pullAndPut.add(setOp);

		return client.bulkWrite(participantCollection, pullAndPut).mapEmpty();
	}
	/**
	 * Get a {@link JsonArray} of {@link JsonObject} elements that contain the state of all participants
	 * (essentially this is a dump of the participants in this study) 
	 * @param project the project for which to retrieve participant data
	 * @return A {@link Future} of the particpant data as a {@link JsonArray} of db entries( {@link JsonObject}s).
	 */
	public Future<JsonArray> getParticipantStatusForProject(Study project)
	{
		Promise<JsonArray> participantsPromise = Promise.promise();
		project.getParticipants()
		.onSuccess(participants -> {
			// if there are no participants yet indicate this
			if(participants.size() == 0)
			{
				participantsPromise.complete(new JsonArray());
				return;
			}
			// For some reason, 
			JsonObject query = new JsonObject().put("_id", new JsonObject().put("$in", participants));																	
			client.findWithOptions(participantCollection,query,new FindOptions().setFields(new JsonObject().put("_id", 1).put("finished", 1)))
			.onSuccess(res -> {				
				JsonArray result = new JsonArray();
				for(JsonObject o : res)
				{
					o.put("participantID", o.getValue("_id")).remove("_id");
					result.add(o);
				}
				LOGGER.debug(result.encodePrettily());		
				participantsPromise.complete(result);
			})
			.onFailure(err -> participantsPromise.fail(err));
		})
		.onFailure(err -> participantsPromise.fail(err));
		return participantsPromise.future();
	}

	/**
	 * Reset the Participant based on the given participants data and empty results. 
	 * @param part the participant to reset
	 * @return a {@link Future} indicating success of the operation
	 */
	public Future<Void> resetParticipant(Participant part)
	{
		Promise<Void> participantsPromise = Promise.promise();
		part.toJson()
		.onSuccess(partJson -> {

			partJson.put("resultData", new JsonArray())
			.put("outputData", new JsonArray())
			.put("modifiedStamp", System.currentTimeMillis());

			client.save(participantCollection, partJson)
			.onSuccess( id -> {
				participantsPromise.complete();
			})
			.onFailure(err -> participantsPromise.fail(err));
		})
		.onFailure(err -> participantsPromise.fail(err));
		return participantsPromise.future();
	}

	/**
	 * TODO: Check if the timestamp from the input data needs to be updated... 
	 * Update the outputs of a participant for the given TaskID and Outputs array.
	 * @param p The {@link Participant} to update
	 * @param taskID the id of the Task for which to update the Outputs
	 * @param Outputs The Output data
	 * @return A Successfull future if the outputs were updated.
	 */
	public Future<Void> updateOutputsForTask(Participant p, String taskID, JsonArray Outputs)
	{
		// We will pull the outputs for this task.
		JsonObject pullUpdate = new JsonObject().put("$pull", new JsonObject()
				.put("outputData", new JsonObject()
						.put("task", taskID)));

		JsonObject pushUpdate = new JsonObject().put("$push", new JsonObject()
				.put("outputData", new JsonObject().put("task", taskID).put("outputs",Outputs)));
		JsonObject setUpdate = new JsonObject().put("$set", new JsonObject()
				.put("modifiedStamp", System.currentTimeMillis()));		
		JsonObject itemQuery = new JsonObject().put("_id", p.getID());
		List<BulkOperation> pullAndPut = new LinkedList<>();
		BulkOperation pullOp = BulkOperation.createUpdate(itemQuery, pullUpdate);
		BulkOperation pushOp = BulkOperation.createUpdate(itemQuery, pushUpdate);
		BulkOperation setOp = BulkOperation.createUpdate(itemQuery, setUpdate);
		pullAndPut.add(pullOp);
		pullAndPut.add(pushOp);				
		pullAndPut.add(setOp);
		return client.bulkWrite(participantCollection, pullAndPut).onSuccess(res -> {
			LOGGER.debug(res.toJson());
		}).mapEmpty();

	}

	/**
	 * TODO: Check if the timestamp from the input data needs to be updated... 
	 * Update the outputs of a participant for the given TaskID and Outputs array.
	 * @param p The {@link Participant} to update
	 * @param Outputs The Output data
	 * @param outputNames the names of the outputs to update
	 * @return A Successfull future if the outputs were updated.
	 */
	public Future<Void> updatePersistentData(Participant p, JsonArray Outputs, JsonArray outputNames)
	{
		// We will pull the outputs for this task.
		JsonObject pullUpdate = new JsonObject().put("$pull", new JsonObject()
				.put("persistentData", new JsonObject()
						.put("name", new JsonObject().put("$in", outputNames))));

		JsonObject pushUpdate = new JsonObject().put("$push", new JsonObject()
				.put("persistentData", new JsonObject().put("$each", Outputs)));
		JsonObject setUpdate = new JsonObject().put("$set", new JsonObject()
				.put("modifiedStamp", System.currentTimeMillis()));		
		JsonObject itemQuery = new JsonObject().put("_id", p.getID());
		List<BulkOperation> pullAndPut = new LinkedList<>();
		BulkOperation pullOp = BulkOperation.createUpdate(itemQuery, pullUpdate);
		BulkOperation pushOp = BulkOperation.createUpdate(itemQuery, pushUpdate);
		BulkOperation setOp = BulkOperation.createUpdate(itemQuery, setUpdate);
		pullAndPut.add(pullOp);
		pullAndPut.add(pushOp);				
		pullAndPut.add(setOp);
		return client.bulkWrite(participantCollection, pullAndPut).onSuccess(res -> {
			LOGGER.debug(res.toJson());
		}).mapEmpty();

	}


	/**
	 * Get the results for the participantIDs indicated in the provided {@link JsonArray}
	 * @param participantIDs The participant IDs for which to obtain data.
	 * @param projectID Nullable. If null or an empty string, participants from multiple projects can be queried simultaneously. 
	 * 				    Otherwise, only the participants that fit to this projectID are returned. 
	 * @return a {@link Future} of {@link JsonObject} which contain information on the participants (_id, steps, resultData and finished).
	 */
	public Future<List<JsonObject>> getParticipantsResults(JsonArray participantIDs, String projectID)
	{
		client.find(participantCollection,new JsonObject()).
		onSuccess(list -> {

			LOGGER.debug("There are " + list.size() + " participants");
			for(JsonObject o : list)
			{
				LOGGER.debug(o.encodePrettily());	
			}
		});
		JsonObject query = new JsonObject().put("_id", new JsonObject().put("$in", participantIDs));
		// if we have a projectID, we need to restrict the query
		// so providing a project ID 
		if(projectID != null && !projectID.equals(""))
		{
			query = new JsonObject().put("$and", new JsonArray().add(query).add(new JsonObject().put("project", projectID)));
		}
		return client.findWithOptions(participantCollection, query , new FindOptions().setFields(new JsonObject().put("_id", 1).put("steps", 1).put("resultData", 1).put("finished", 1)));		
	}

	/**
	 * Get the results for the participantIDs indicated in the provided study
	 * @param participantIDs a list of participant IDs
	 * @param studyID the id of the study
	 * @param TaskID the id of the task for which to get data
	 * @return A {@link Future} of a List of JsonObjects containing the results for the given participants in the given project and task
	 */
	public Future<List<JsonObject>> getParticipantsResultsForTask(JsonArray participantIDs, String studyID, String TaskID)
	{

		JsonObject query = new JsonObject().put("$and", new JsonArray().add(new JsonObject().put("_id", new JsonObject().put("$in", participantIDs)))
				.add(new JsonObject().put("resultData.task", new JsonObject().put("$eq", TaskID)))
				);
		// if we have a projectID, we need to restrict the query
		if(studyID != null && !studyID.equals(""))
		{
			query = new JsonObject().put("$and", new JsonArray().add(query).add(new JsonObject().put("project", studyID)));
		}
		query = new JsonObject().put("$match",query);		
		JsonObject dataAddReArr = new JsonObject().put("$set", new JsonObject().put("resultData", new JsonObject().put("participantID", "$_id")));
		JsonObject dataProj = new JsonObject().put("$project", new JsonObject().put("participantData", "$resultData").put("_id", 0));
		return MongoAggregationHandler.aggregate(client, participantCollection, new JsonArray().add(query).add(dataAddReArr).add(dataProj));		
	}

	/**
	 * Get the participant ID associated with the provided Token.
	 * @param token the provided token
	 * @param projectID the project ID (as check that ID and project match).
	 * @return A Future of the {@link Participant}s ID for the given Token in the Study
	 */
	public Future<String> getParticipantIDForToken(String token, String projectID)
	{
		Promise<String> IDPromise = Promise.promise();

		client.findOne(participantCollection, new JsonObject().put("token", token).put("project", projectID), null)
		.onSuccess(res -> {
			if(res == null)
			{
				IDPromise.fail(new UserDoesNotExistException(token));
			}
			else
			{
				IDPromise.complete(res.getString("_id"));
			}							
		})
		.onFailure(err -> IDPromise.fail(err));
		return IDPromise.future();
	}


}
