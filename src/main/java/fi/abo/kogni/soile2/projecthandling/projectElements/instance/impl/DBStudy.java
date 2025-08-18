package fi.abo.kogni.soile2.projecthandling.projectElements.instance.impl;



import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.projecthandling.apielements.APIElement;
import fi.abo.kogni.soile2.projecthandling.apielements.APIProject;
import fi.abo.kogni.soile2.projecthandling.exceptions.ObjectDoesNotExist;
import fi.abo.kogni.soile2.projecthandling.exceptions.ProjectIsInactiveException;
import fi.abo.kogni.soile2.projecthandling.participant.Participant;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Project;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.Study;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.prng.VertxContextPRNG;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.handler.HttpException;
/**
 * This is a Project stored in a (most likely git) Database.
 * Both the participants field and the tokens fields are independent of the the this Object and always directly retrieved from the database
 *   
 * @author thomas
 *
 */
public class DBStudy extends Study{

	MongoClient client;
	EventBus eb;
	ElementManager<Project> projectManager;	
	static final Logger LOGGER = LogManager.getLogger(DBStudy.class);
	/**
	 * Default constructor
	 * @param projManager Element Manager for Project retrieval
	 * @param client {@link MongoClient} for db access 
	 * @param eb {@link EventBus} for communication
	 */
	public DBStudy(ElementManager<Project> projManager, MongoClient client, EventBus eb) {
		super();
		this.projectManager = projManager;
		this.client = client;
		this.eb = eb;
	}

	
	
	@Override
	public Future<JsonObject> save(boolean sourceProjectChanged) {
		Promise<JsonObject> saveSuccess = Promise.<JsonObject>promise();
		//TODO: If we at some point allow later modification of Name and shortcut, we need to add checks here that they do not conflict
		// For an implementation have a look at @ElementToDBProjectInstance
		// remove id.
		
		JsonObject update = new JsonObject().put("$set", toDBJson());		
		update.remove("_id");		
		//first check, that this change does not interfere with another object.
		JsonObject query = new JsonObject();
		if(shortcut != null && !"".equals(shortcut)) // if it's a shortcut and not "" (indicating no shortcut)
		{
			// the shortcut is not allowed to clash with either the IDs OR other shortcuts 
			query.put("$or", new JsonArray().add(new JsonObject()
													 .put("name",getName()))
											.add(new JsonObject()
													.put("shortcut",shortcut))
											.add(new JsonObject()
													.put("_id",shortcut)));
		}
		else
		{
			query.put("name",getName());
		}
		client.find(getTargetCollection(),query)
		.onSuccess(res -> 
		{
			// this should only bring up the entry corresponding to this element. 
			if(res.size() > 1)
			{
				saveSuccess.fail(new HttpException(409,"Shortcut or name in use by another project"));
				return;
			}
			else
			{
				if(res.size() == 1)
				{
					// check that this is the correct object.
					if(!res.get(0).getString("_id").equals(instanceID))
					{
						saveSuccess.fail(new HttpException(409,"Shortcut or name in use by another project"));
						return;
					}									
				}
				JsonObject updateQuery = new JsonObject().put("_id", instanceID);
				// Not sure if this is the best way to do this, but this allows copying the least amount of code...
				// essentially we check twice, whether the sourceProject was changed and if it was, we alter the update
				Future<APIElement<Project>> sourceProject;
				if(sourceProjectChanged)
				{
					sourceProject = projectManager.getAPIElementFromDB(getSourceUUID(), getSourceVersion());
				}
				else
				{
					sourceProject = Future.succeededFuture(null);
				}
				sourceProject.compose(apiProject -> {					
					if(sourceProjectChanged)
					{
						// so the source was changed, we need to update the randomizerPasses, as we cannot rely on them any more.
						update.getJsonObject("$set").put("randomizerPasses", createRandomizerArray(((APIProject)apiProject).getRandomizers()));
					}
					return client.updateCollection(getTargetCollection(), updateQuery, update);
				}).onSuccess(result ->
				{			
					saveSuccess.complete(toDBJson());					
				}).onFailure(fail ->{
					saveSuccess.fail(fail);
				});
			}
				
		})
		.onFailure(err -> saveSuccess.fail(err));
		
				
		return saveSuccess.future();		
	}

	/**
	 * This loader expects that the input json is a query for the mongo db of instances.
	 * It will then extract the remaining data first from the Project Instance database and then from the git repository.
	 */
	@Override
	public Future<JsonObject> load(JsonObject instanceInfo) {
		Promise<JsonObject> loadSuccess = Promise.<JsonObject>promise();
		LOGGER.debug("Trying to load from DB: \n" + instanceInfo.encodePrettily());		
		client.findOne(getTargetCollection(), instanceInfo, null).onSuccess(instanceJson -> {
			LOGGER.debug("Found an entry");
			if(instanceJson == null)
			{				
				loadSuccess.fail(new ObjectDoesNotExist(instanceID));
			}
			else
			{				
				LOGGER.debug(instanceJson.encodePrettily());									
				projectManager.getGitJson(instanceJson.getString("sourceUUID"),instanceJson.getString("version"))
				.onSuccess(projectData -> 
				{
					LOGGER.debug("The data from the project git file is: \n" + projectData.encodePrettily());
					// we got a positive reply.
					// this is only the git data, so we need to set the UUID/version information
					projectData.put("UUID",instanceJson.getString("sourceUUID"));
					projectData.put("version",instanceJson.getString("version"));
					instanceJson.put("sourceProject", projectData);					
					LOGGER.debug(instanceJson.encodePrettily());	
					loadSuccess.complete(instanceJson);					
				}).onFailure(fail -> {
					LOGGER.debug("Loading git File failed");
					LOGGER.trace("Could no load");
					loadSuccess.fail(fail);							
				});
			}
		}).onFailure(fail -> {
			loadSuccess.fail(fail);
		});
		return loadSuccess.future();		
	}

	@Override
	public Future<JsonObject> delete() {
		JsonObject query = new JsonObject().put("_id", this.instanceID);
		return client.findOneAndDelete(getTargetCollection(), query);		
	}
	
	@Override
	public Future<JsonArray> reset() {
		JsonObject query = new JsonObject().put("_id", this.instanceID);
		Promise<JsonArray> resetPromise = Promise.<JsonArray>promise();
		projectManager.getAPIElementFromDB(this.getSourceUUID(), this.getSourceVersion())
		.onSuccess(apiProject -> {
		client.findOneAndUpdate(getTargetCollection(), query, new JsonObject().put("$set",new JsonObject().put("participants", new JsonArray())
																										  .put("randomizerPasses", createRandomizerArray(((APIProject)apiProject).getRandomizers())))																			  
																			  .put("$unset", new JsonObject().put("usedTokens", "")
																					  						 .put("signupTokens","")
																					  						 .put("permanentAccessToken", "")))
		.onSuccess(result -> {
			resetPromise.complete(result.getJsonArray("participants", new JsonArray()));
		})
		.onFailure(err -> resetPromise.fail(err));
		})
		.onFailure(err -> resetPromise.fail(err));
		
		return resetPromise.future();
	}

	/**
	 * Add a participant to the list of participants of this projects
	 * @param p the participant to add
	 */
	@Override
	public synchronized Future<Void> addParticipant(Participant p)
	{
		Promise<Void> updatePromise = Promise.promise();
		isActive()
		.onSuccess(active -> {
			if(!active)
			{
				updatePromise.fail(new ProjectIsInactiveException(name));
			}
			else
			{
				JsonObject update = new JsonObject().put("$push", new JsonObject().put("participants",p.getID()));
				client.updateCollection(getTargetCollection(), new JsonObject().put("_id", instanceID), update )
				.onSuccess(res -> {
					if(res.getDocModified() != 1)
					{
						LOGGER.error("Modified multiple object while only one ID was provided! Project was: " + instanceID );
						updatePromise.fail("Mongo Error");

					}
					else
					{
						updatePromise.complete();
					}
				})
				.onFailure(err -> updatePromise.fail(err));	
			}
		})
		.onFailure(err -> updatePromise.fail(err));
					
		return updatePromise.future();
	}

	/**
	 * Delete a participant from the list of participants of this projects
	 * @param p the participant to remove
	 */
	@Override
	public synchronized Future<Boolean> deleteParticipant(Participant p)
	{
		Promise<Boolean> updatePromise = Promise.promise();
		JsonObject update = new JsonObject().put("$pull", new JsonObject().put("participants",p.getID()));
		client.updateCollection(getTargetCollection(), new JsonObject().put("_id", instanceID), update )
		.onSuccess(res -> {
			if(res.getDocModified() != 1)
			{
				
				if(res.getDocModified() == 0 )
				{
					LOGGER.error("Nothing was changed, participant did not exist in project: " + instanceID );
					updatePromise.complete(false);
				}
				else
				{
					LOGGER.error("Modified multiple objects or none while only one ID was provided! Project was: " + instanceID );
					updatePromise.fail("Mongo Error");
				}

			}
			else
			{
				updatePromise.complete(true);
			}
		})
		.onFailure(err -> updatePromise.fail(err));		
		return updatePromise.future();
	}


	@Override
	public synchronized Future<JsonArray> getParticipants()
	{
		Promise<JsonArray> listPromise = Promise.promise();

		client.findOne(getTargetCollection(), new JsonObject().put("_id", instanceID), new JsonObject().put("participants", 1) )
		.onSuccess(res -> {								
			listPromise.complete(res.getJsonArray("participants"));
		})
		.onFailure(err -> listPromise.fail(err));		
		return listPromise.future();		
	}

	@Override
	public Future<Void> deactivate()
	{
		return client.findOneAndUpdate(getTargetCollection(), createStudyQuery(), new JsonObject().put("$set", new JsonObject().put("active", false))).mapEmpty();		
	}

	@Override
	public Future<Void> activate()
	{
		return client.findOneAndUpdate(getTargetCollection(), createStudyQuery(), new JsonObject().put("$set", new JsonObject().put("active", true))).mapEmpty();		
	}
	
	@Override
	public Future<Boolean> isActive()
	{
		return client.findOne(getTargetCollection(), createStudyQuery(), new JsonObject().put("active", 1)).map(activitistate -> activitistate.getBoolean("active", true));		
	}
	
	private JsonObject createStudyQuery()
	{
		return new JsonObject().put("_id", getID());
	}
	
	@Override
	public Future<JsonArray> createSignupTokens(int count) {		 
		Promise<JsonArray> tokenPromise = Promise.promise();				
		JsonObject query = new JsonObject().put("_id", instanceID);
		client.findOne(getTargetCollection(), query, new JsonObject().put("signupTokens", 1).put("usedTokens", 1))
		.onSuccess( res -> {
			// TODO: Check, whether there are more efficient ways to do this...
			JsonArray newTokens = new JsonArray();
			Set<Object> currentTokens = new HashSet<Object>();
			for(Object o : res.getJsonArray("signupTokens", new JsonArray()))
			{
				currentTokens.add(o);
			}
			for(Object o : res.getJsonArray("usedTokens", new JsonArray()))
			{
				currentTokens.add(o);
			}
			int previoussize = currentTokens.size();
			LOGGER.debug("Previously there were: " + currentTokens.size() + " tokens. " + count + " tokens will be created");
			while(currentTokens.size() < previoussize + count)
			{
				VertxContextPRNG rng = VertxContextPRNG.current();
				int currentSize = currentTokens.size();
				String nextToken = rng.nextString(30).substring(0, 30);
				LOGGER.debug("Token " + nextToken +" has a length of: " + nextToken.length());
				currentTokens.add(nextToken);
				if(currentTokens.size() > currentSize)
				{
					newTokens.add(nextToken);
				}					
			}			
			LOGGER.debug("New tokens are:\n" + newTokens.encodePrettily());
			client.updateCollection(getTargetCollection(), query, new JsonObject().put("$push", new JsonObject().put("signupTokens",new JsonObject().put("$each", newTokens))))
			.onSuccess( updated -> {

				tokenPromise.complete(newTokens);
			})
			.onFailure(err -> tokenPromise.fail(err));		
		})
		.onFailure(err -> tokenPromise.fail(err));
		return tokenPromise.future();

	}

	@Override
	public Future<String> createPermanentAccessToken() {
		JsonObject query = new JsonObject().put("_id", instanceID);
		VertxContextPRNG rng = VertxContextPRNG.current();		
		String Token = rng.nextString(40).substring(0, 40);
		return client.updateCollection(getTargetCollection(), query, new JsonObject().put("$set", new JsonObject().put("permanentAccessToken",Token))).map(Token);				
	}

	@Override
	public Future<Void> useToken(String token) {
		LOGGER.debug("Trying to validate token: " + token);
		Promise<Void> tokenUsedPromise = Promise.promise();
		JsonObject query = new JsonObject().put("_id", instanceID);
		if(token.length() == 30)
		{
			LOGGER.debug("Testing single use Access Token");
			client.updateCollection(getTargetCollection(), query, new JsonObject().put("$pull", new JsonObject().put("signupTokens",token)))
			.onSuccess( updated -> {
				LOGGER.debug(updated.toJson().encodePrettily());
				if(updated.getDocModified() == 0)
				{
					tokenUsedPromise.fail(new HttpException(403, "Invalid Token, or token already used"));
				}
				else
				{
					client.updateCollection(getTargetCollection(), query, new JsonObject().put("$push", new JsonObject().put("usedTokens",token)))
					.onSuccess( tokenUsed -> {
						tokenUsedPromise.complete();
					})
					.onFailure(err -> tokenUsedPromise.fail(err));
				}
		})
		.onFailure(err -> tokenUsedPromise.fail(err));
		}
		else
		{
			LOGGER.debug("Testing permanent Access Token");
			client.findOne(getTargetCollection(), query.put("permanentAccessToken",token), null)
			.onSuccess( res -> {
				if(res == null)
				{
					tokenUsedPromise.fail(new HttpException(403, "Invalid Token"));
				}
				else
				{
					tokenUsedPromise.complete();
				}
			})
			.onFailure(err -> tokenUsedPromise.fail(err));
		}
		return tokenUsedPromise.future();
	}

	@Override
	public Future<JsonObject> getTokenInformation() {
		return client.findOne(getTargetCollection(), new JsonObject().put("_id", getID()), new JsonObject().put("signupTokens", 1).put("permanentAccessToken", 1).put("usedTokens", 1))
				.map(data -> { 
					JsonObject result = new JsonObject();
					result.put("signupTokens", data.getJsonArray("signupTokens",new JsonArray()))
						  .put("permanentAccessToken", data.getString("permanentAccessToken",""))
						  .put("usedTokens", data.getJsonArray("usedTokens",new JsonArray()));
					return result;
					});			
	}
	
	@Override
	public FieldSpecifications getUpdateableDBFields() {
		
		return new FieldSpecifications().put(new FieldSpecification("description", String.class, () -> "", true))
				.put(new FieldSpecification("shortDescription", String.class, () -> "", false))
				.put(new FieldSpecification("description", String.class, () -> "", false))
				.put(new FieldSpecification("private", Boolean.class, () -> true, false))				
				.put(new FieldSpecification("sourceUUID", String.class, () -> getSourceUUID(), true))
				.put(new FieldSpecification("version", String.class, () -> getSourceVersion(), true));				

	}
	/**
	 * Get unmodifyable fields in a study
	 * @return {@link FieldSpecifications} of unmodifyable fields
	 */
	public FieldSpecifications getUnmodifyableDBFields() {
		
		return new FieldSpecifications().put(new FieldSpecification("_id", String.class, () -> getID(), true))
				.put(new FieldSpecification("participants", JsonArray.class, () -> this.getParticipants().result(), false))
				.put(new FieldSpecification("name", String.class, () -> getName(), false));
								

	}

	@Override
	protected Future<Void> checkChangeAllowed(JsonObject updateData) {
		
		Promise<Void> canChangePromise = Promise.promise();
		String newShortCut = updateData.getString("shortcut","");
		String newName = updateData.getString("name",this.name);
		if((newShortCut.equals(this.shortcut) || newShortCut.equals("")) && newName.equals(this.name))
		{
			// Name stays the same, shortcut is either the old shortcut or set to empty. 
			return Future.succeededFuture();
		}
		JsonObject query = new JsonObject().put("$and", new JsonArray()
															.add(new JsonObject().put("$or", new JsonArray().add(new JsonObject().put("shortcut", newShortCut))
																											.add(new JsonObject().put("name", newName))))
															.add(new JsonObject().put("_id", new JsonObject().put("$ne", getID())))
												);
		client.findOne(getTargetCollection(), query, null)
		.onSuccess(res -> {
			if(res == null)
			{
				canChangePromise.complete();
			}				
			else
			{
				if(res.getString("name").equals(newName))
				{
					canChangePromise.fail(new HttpException(409, "Study with this name already exists, you might not have access to it."));					
				}
				else
				{
					canChangePromise.fail(new HttpException(409, "Study with this shortcut already exists, you might not have access to it."));
				}
			}
			});
		return canChangePromise.future();
	}

	@Override
	public Future<Long> getStoredModificationDate() {
		return client.findOne(getTargetCollection(), new JsonObject().put("_id", getID()), new JsonObject().put("modifiedStamp", 1))
		.map(data -> data.getLong("modifiedStamp"));	
	}
	
	/**
	 * create a randomizer array indicating the number of participants who passed this 
	 * for a given set of randomizer objects from a project json.
	 * @param randomizers The randomizers JsonArray from a 
	 * @return the objct to be stored in the study.
	 */
	protected JsonObject createRandomizerArray(JsonArray randomizers)
	{
		JsonObject result = new JsonObject();
		for(int i = 0; i < randomizers.size(); ++i)
		{
			result.put(randomizers.getJsonObject(i).getString("instanceID"), 0);
		}
		return result;
	}



	@Override
	public Future<Integer> getFilterPassesAndAddOne(String filterID) {		
		JsonObject update = new JsonObject().put("$inc", new JsonObject().put("randomizerPasses." + filterID,1));		
		return client.findOneAndUpdate(getTargetCollection(), new JsonObject().put("_id", getID()), update).map(targetObject -> 
			targetObject.getJsonObject("randomizerPasses").getInteger(filterID)									
		);		
	}

}
