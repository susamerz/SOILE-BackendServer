package fi.abo.kogni.soile2.projecthandling.participant;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.datalake.ParticipantFileResults;
import fi.abo.kogni.soile2.datamanagement.utils.CheckDirtyMap;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.Study;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.impl.StudyHandler;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
/**
 * The participantHandler keeps track of all participants. 
 * It stores 
 * @author Thomas Pfau
 *
 */
public class ParticipantHandler {
	MongoClient client;
	StudyHandler studyHandler;
	CheckDirtyMap<String,Participant> activeparticipants;	
	ParticipantManager manager;
	Vertx vertx;
	String dataLakeFolder;
	static final Logger LOGGER = LogManager.getLogger(ParticipantHandler.class);



	/**
	 * Default constructor
	 * @param client a {@link MongoClient} for db communication
	 * @param studyHandler the {@link StudyHandler} for study access 
	 * @param vertx The {@link Vertx} instance for communication
	 */
	public ParticipantHandler(MongoClient client, StudyHandler studyHandler, Vertx vertx) {
		super();		
		this.client = client;
		this.studyHandler = studyHandler;
		this.manager = new ParticipantManager(client);
		this.vertx = vertx;
		activeparticipants = new CheckDirtyMap<String, Participant>(manager, 2*3600); //Keep for two hours
		dataLakeFolder = SoileConfigLoader.getServerProperty("soileResultDirectory");
	}

	
	/**
	 * Create a normal Participant in the given Study. 
	 * @param study the {@link Study} to create a participant in
	 * @return the Created {@link Participant}
	 */
	public Future<Participant> create(Study study)
	{
		return createParticipant(study, "", false);
	}
	
	
	/**
	 * Create a normal Participant in the Study represented by the given ID. 
	 * @param studyID the ID of the {@link Study} to create a participant for.
	 * @return the Created {@link Participant}
	 */
	public Future<Participant> create(String studyID)
	{
		return studyHandler.loadUpToDateStudy(studyID)
		.compose(study -> createParticipant(study, "", false));		
	}
	/**
	 * Create a participant in the database
	 * @param p the {@link Study} for which to create a participant
	 * @param signuptoken the token used for signup
	 * @param TokenParticipant whether this will be a Token participant
	 * @return a {@link Future} of the {@link Participant} that was created
	 */
	public Future<Participant> createParticipant(Study p, String signuptoken, boolean TokenParticipant)
	{
		return manager.createParticipant(p, signuptoken, TokenParticipant);
	}
	
	/**
	 * Clean up the data currently stored by this Participant handler. 
	 * This is necessary to avoid excessive data in memory.
	 */
	public void cleanup()
	{
		activeparticipants.cleanup();
	}

	/**
	 * Retrieve a participant from the database (or memory) and return the participant
	 * based on the participants uID.
	 * @param id the uid of the participant
	 * @param handler the handler that requested the participant.
	 */
	public void getParticpant(String id, Handler<AsyncResult<Participant>> handler)
	{
		activeparticipants.getData(id, handler);
	}

	/**
	 * Retrieve a participant from the database (or memory) and return the participant
	 * based on the participants uID.
	 * @param id the uid of the participant
	 * @return A {@link Future} of the requested {@link Participant} 
	 */
	public Future<Participant> getParticipant(String id)
	{
		return activeparticipants.getData(id);		
	}

	/**
	 * Retrieve a participant from the database (or memory) and return the participant
	 * based on the participants uID.
	 * @param token the token to use for the {@link Participant}
	 * @param studyID the ID of the {@link Study} to create a {@link Participant} in
	 * @return a {@link Future} of the {@link Participant}
	 */
	public Future<Participant> getParticipantForToken(String token, String studyID)
	{
		Promise<Participant> partPromise = Promise.<Participant>promise();
		manager.getParticipantIDForToken(token, studyID)
		.onSuccess(id -> {
			getParticipant(id)
			.onSuccess(participant -> {
				partPromise.complete(participant);
			})
			.onFailure(err -> partPromise.fail(err));
		})
		.onFailure(err -> partPromise.fail(err));
		return partPromise.future();
	}




	/**
	 * Delete a participant and all data associated with the participant from the project.
	 * @param id the id of the participant that is to be deleted
	 * @param participantHasToBeInStudy whether the participant has to be in a study
	 * @return A {@link Future} indicating whether the operation was successful
	 */
	public Future<Void> deleteParticipant(String id, boolean participantHasToBeInStudy)
	{
		Promise<Void> deletionPromise = Promise.<Void>promise();
		// all Files for a participant are stored in the folder: datalake/PARTICIPANTID
		getParticipant(id)
		.onSuccess( participant -> {
			ParticipantFileResults results = new ParticipantFileResults(id);
			
			vertx.fileSystem().exists(results.getParticipantFolderPath(dataLakeFolder))
			.onSuccess(deleteFiles -> {
				
				if(deleteFiles)
				{
					vertx.fileSystem().deleteRecursive(results.getParticipantFolderPath(dataLakeFolder))
					.onSuccess(filesDeleted -> 			
					{
						//TODO: Need to change this, so that it is FIRST removed from the Study and THEN deleted from the participant db.... 
						removeParticipantFromManager(participant, id, participantHasToBeInStudy)
						.onSuccess(done -> deletionPromise.complete())
						.onFailure(err -> deletionPromise.fail(err));
						// now, all files and folders have been removed. So we will delete the participant ID.
						// TODO: Do we need to handle user participants, probably?
					})
					.onFailure(
					err -> {						
						LOGGER.error("Error while deleting files for participant " + id + "!");
						LOGGER.error(err);
						deletionPromise.fail(err);	
					});
				}
				else
				{
					removeParticipantFromManager(participant, id, participantHasToBeInStudy)
					.onSuccess(done -> deletionPromise.complete())
					.onFailure(err -> deletionPromise.fail(err));
				}
			}).onFailure(
			err -> {						
				LOGGER.error("Error while determining if files exist!");
				LOGGER.error(err);
				deletionPromise.fail(err);	
			});
		})
		.onFailure(err -> deletionPromise.fail(err));			
		return deletionPromise.future();
	}

	private Future<Void> removeParticipantFromManager(Participant participant, String id, boolean participantHasToBeInStudy)
	{
		Promise<Void> removedPromise = Promise.promise();
		studyHandler.removeParticipant(participant.getStudyID(), participant, participantHasToBeInStudy)
		.onFailure(err -> {
			LOGGER.error("Error while deleting participant " + id + ". Couldnt remove from the Project !");
			LOGGER.error(err); 
		})
		.compose( success -> {
			return manager.deleteParticipant(id);
		})
		.onFailure(err -> {
			LOGGER.error("Error while deleting participant " + id + ". Couldnt remove from the participant database!");
			LOGGER.error(err);				
		})
		.compose(managerRemoved -> {
			// remove from the User, if it was a user participant.
			if(participant.hasToken())
			{
				return Future.succeededFuture();
			}
			// not a token participant so we need to look it up from the user database and pull it from whoever has it. 
			return vertx.eventBus().request("soile.umanager.removeParticipantInStudy", new JsonObject().put("participantID", participant.getID()).put("studyID", participant.getStudyID()));
		})
		.onFailure(err -> removedPromise.fail(err))
		.onSuccess(deletionSuccess -> {

				activeparticipants.cleanElement(id);
				removedPromise.complete();
			});									
		
		return removedPromise.future();
	}

	/**
	 * Retrieve the status of all participants in the given study (the db extract of all participants)
	 * @param project the study to get the status in
	 * @return A Future of the db extract {@link JsonArray} of {@link JsonObject}s
	 */
	public Future<JsonArray> getParticipantStatusForProject(Study project)
	{
		return manager.getParticipantStatusForProject(project);
	}

	/**
	 * Get a {@link JsonArray} of {@link JsonObject} elements that contain the 
	 * @param participantIDs A List of partiicpant ids
	 * @param taskID the task id for which to get the info
	 * @param projectID the Study id
	 * @return A {@link Future} of a List of Participant data.
	 */
	public Future<List<JsonObject>> getTaskDataforParticipants(JsonArray participantIDs, String taskID, String projectID)
	{
		return manager.getParticipantsResultsForTask(participantIDs, projectID, taskID);
	}

	/**
	 * Get a {@link List} of {@link JsonObject} elements that contain the results along with some additional information for each participant. 
	 * @param project the study for which to get data
	 * @param particpantIDs the IDs of the participants for which to get data
	 * @return A {@link Future} of a List of Participant data.
	 */
	public Future<List<JsonObject>> getParticipantData(Study project, JsonArray particpantIDs)
	{
		Promise<List<JsonObject>> dataPromise = Promise.<List<JsonObject>>promise();
		if(particpantIDs == null)
		{// this is a request for all Participants, so just collect the data.
			project.getParticipants().onSuccess(participants -> 
			{				
				getParticipantData(project, participants)
				.onSuccess(res -> 
				{
					dataPromise.complete(res);
				})
				.onFailure(err -> dataPromise.fail(err));
			})
			.onFailure(err -> dataPromise.fail(err));

		}
		else
		{
			manager.getParticipantsResults(particpantIDs, project.getID())
			.onSuccess( res -> {
				dataPromise.complete(res);
			})
			.onFailure(err -> dataPromise.fail(err));

		}
		return dataPromise.future();
	}	
}
