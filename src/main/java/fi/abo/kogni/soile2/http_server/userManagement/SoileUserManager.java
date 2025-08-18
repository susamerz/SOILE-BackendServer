package fi.abo.kogni.soile2.http_server.userManagement;

import static io.vertx.ext.auth.impl.Codec.base64Encode;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.PermissionType;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.http_server.auth.SoilePermissionProvider;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.CannotUpdateMultipleException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.DuplicateUserEntryInDBException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.EmailAlreadyInUseException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.InvalidEmailAddress;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.InvalidRoleException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.UserAlreadyExistingException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.UserDoesNotExistException;
import fi.abo.kogni.soile2.projecthandling.participant.Participant;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.hashing.HashingStrategy;
import io.vertx.ext.auth.mongo.MongoAuthenticationOptions;
import io.vertx.ext.auth.mongo.MongoAuthorizationOptions;
import io.vertx.ext.auth.mongo.MongoUserUtil;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;


//TODO: Reactor this to actually create futures. 

/**
 * This class encapsulates all activity correlating to user management interaction with the MongoDB database. 
 *	This includes authorization, authentication as well as user generation, and password hashing.  
 * @author thomas
 *
 */
public class SoileUserManager implements MongoUserUtil{

	private static Logger LOGGER = LogManager.getLogger(SoileUserManager.class.getName()); 
	private final MongoClient client;
	private final HashingStrategy strategy;	
	private final SecureRandom random = new SecureRandom();
	private final MongoAuthenticationOptions authnOptions;
	private final MongoAuthorizationOptions authzOptions;
	private final String hashingAlgorithm;
	private HashMap<PermissionChange, String> permissionMap;
	/**
	 * Types of permission changes
	 * @author Thomas Pfau
	 *
	 */
	public static enum PermissionChange{
		/**
		 * Remove a permission
		 */
		Remove,
		/**
		 * Add a permission
		 */
		Add,
		/**
		 * Set (replacing existing) permissions
		 */
		Set,
		/**
		 * Update / alter existing permissions
		 */
		Update
	}



	/**
	 * Default constructor
	 * @param client The {@link MongoClient} for db interaction
	 */
	public SoileUserManager(MongoClient client) {
		this.client = client;
		this.authnOptions = SoileConfigLoader.getMongoAuthNOptions();
		this.authzOptions = SoileConfigLoader.getMongoAuthZOptions();		
		strategy = new SoileHashing(SoileConfigLoader.getUserProperty("serverSalt"));
		hashingAlgorithm = SoileConfigLoader.getUserProperty("hashingAlgorithm");
		permissionMap = new HashMap<>();
		permissionMap.put(PermissionChange.Remove, "$pull");
		permissionMap.put(PermissionChange.Add, "$push");
		permissionMap.put(PermissionChange.Set, "$set");
	}

	/**
	 * Set up the DB (i.e. making the usernameField a unique index.
	 * @return A {@link Future} that is successfull if the db was set up successfully
	 */
	public Future<Void> setupDB()
	{		
		IndexOptions options = new IndexOptions();
		options.unique(true);			
		return client.createIndexWithOptions(authnOptions.getCollectionName(), 
				new JsonObject().put(authnOptions.getUsernameField(), "text"), options).compose((Void) -> {
					return client.listIndexes(authnOptions.getCollectionName()).onSuccess(indexArray -> {
						LOGGER.debug(indexArray.encodePrettily());
					}).mapEmpty();
				});
	}
	/**
	 * Check whether the Email listed is present in the email list of the database.
	 * The handler needs to handle the AsyncResult that is the number of entries with that email.
	 * @param email The email address to check
	 * @param handler the {@link Handler} to handle the result of the call 
	 * @return this object for chained commands
	 */
	public SoileUserManager checkEmailPresent(String email, Handler<AsyncResult<Boolean>> handler)
	{		
		handler.handle(isEmailInUse(email, null));
		return this;
	}

	/**
	 * Check whether the Email listed is present in the email list of the database.
	 * The handler needs to handle the AsyncResult that is the number of entries with that email.
	 * @param email The email address to check
	 * @param excludeUser A user to exclude for the lookup
	 * @return A {@link Future} that contains a boolean inidcating whether the email is in use
	 */
	public Future<Boolean> isEmailInUse(String email, String excludeUser)
	{
		if( email == null)
		{
			// there is no email so it can't be in use.
			return Future.succeededFuture(false);
		}
		JsonObject emailQuery = new JsonObject().put(SoileConfigLoader.getUserdbField("userEmailField"), email.toLowerCase());						
		if(excludeUser != null)
		{
			emailQuery = new JsonObject().put("$and", new JsonArray().add(emailQuery)
																	 .add(new JsonObject().put(authnOptions.getUsernameField(), new JsonObject().put("$not", new JsonObject().put("$eq", excludeUser)))));
		}
		Promise<Boolean> presentPromise = Promise.<Boolean>promise();
		client.count(authzOptions.getCollectionName(), emailQuery)
		.onSuccess(count -> {
			presentPromise.complete(count == 1L);
		})
		.onFailure(err -> presentPromise.fail(err));		
		return presentPromise.future();
	}

	/**
	 * Get the list of users, 
	 * @param skip how many results to skip
	 * @param limit how many results at most to return
	 * @param query a search query to look up in the usernames
	 * @param type The type of user (i.e. their role, Researcher, Participant, Admin), providing User will return all Researchers and Admins
	 * @param namesOnly only list the names
	 * @return A List of JsonObjects with the results.
	 */
	public Future<JsonArray> getUserList(Integer skip, Integer limit, String query, String type, boolean namesOnly)
	{
		JsonObject fields = new JsonObject().put("_id", 0)
				.put(authnOptions.getUsernameField(), 1);
		if(!namesOnly)
		{
			fields
			.put(SoileConfigLoader.getUserdbField("userEmailField"), 1)
			.put(SoileConfigLoader.getUserdbField("userFullNameField"),1)		  
			.put(authzOptions.getRoleField(), 1);
		}
		FindOptions options = new FindOptions();
		JsonArray QueryElements = new JsonArray();
		JsonObject DBQuery = new JsonObject();		
		if(query != null)
		{
			JsonObject searchQuery = new JsonObject().put("$regex",  Pattern.quote(query)).put("$options", "i");
			JsonArray orQuery = new JsonArray()
								.add(new JsonObject().put(SoileConfigLoader.getUserdbField("userFullNameField"), searchQuery))
								.add(new JsonObject().put(SoileConfigLoader.getUserdbField("userEmailField"), searchQuery))
								.add(new JsonObject().put(authnOptions.getUsernameField(), searchQuery));
			QueryElements.add(new JsonObject().put("$or",orQuery));			
		}
		if(type != null)
		{
			JsonObject typeRestriction = new JsonObject();
			if(type.equals("User")) // special case
			{
				typeRestriction.put(authzOptions.getRoleField(), new JsonObject().put("$elemMatch", new JsonObject().put("$in", new JsonArray().add(Roles.Admin.toString()).add(Roles.Researcher.toString()))));
			}
			else
			{
				typeRestriction.put(authzOptions.getRoleField(), new JsonObject().put("$elemMatch", new JsonObject().put("$eq", type)));
			}
			QueryElements.add(typeRestriction);
		}
		if(QueryElements.size() > 0)
		{
			if(QueryElements.size() == 1)
			{
				DBQuery = QueryElements.getJsonObject(0);
			}
			else
			{
				DBQuery.put("$and", QueryElements);
			}
		}
		if(limit != null)
		{
			options.setLimit(limit);
		}
		if(skip != null)
		{
			options.setSkip(skip);
		}
		options.setFields(fields);
		
		Promise<JsonArray> resultsPromise = Promise.promise();
		LOGGER.debug(DBQuery.encodePrettily());
		client.findWithOptions(authnOptions.getCollectionName(), DBQuery, options)
		.onSuccess(result -> {
			LOGGER.debug(result);
			if(!namesOnly)
			{
				for(JsonObject current : result)
				{
					current.put(authzOptions.getRoleField(), current.getJsonArray(authzOptions.getRoleField()).getValue(0));
				}
			}
			resultsPromise.complete(new JsonArray(result));
		})
		.onFailure(err -> resultsPromise.fail(err));
		return resultsPromise.future();
	}

	/**
	 * Get the list of users, 
	 * @param skip how many results to skip
	 * @param limit how many results at most to return
	 * @param query a search query to look up in the usernames
	 * @param type The type of user (i.e. their role, Researcher, Participant, Admin), providing User will return all Researchers and Admins
	 * @param namesOnly only list the names
	 * @param handler {@link Handler} for the result 
	 * @return this object
	 */
	public SoileUserManager getUserList(Integer skip, Integer limit, String query, String type, boolean namesOnly, Handler<AsyncResult<JsonArray>> handler)
	{
		handler.handle(getUserList(skip, limit, query, type, namesOnly));
		return this;
	}



	/**
	 * Change roles or permissions indicating the correct field of the database. 
	 * @param username the id to add the roles/permissions for.
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param permissions the list of roles or permissions to change
	 * @param alterationFlag Whether to add, remove or replace the indicated permissions. 
	 * @param resultHandler the handler for the results.	 
	 * @return this  object
	 */
	public SoileUserManager changePermissions(String username, MongoAuthorizationOptions options, JsonArray permissions, PermissionChange alterationFlag, Handler<AsyncResult<Void>> resultHandler)
	{

		LOGGER.debug("Trying to update");
		JsonObject queryObject = new JsonObject().put(options.getUsernameField(), username);		
		try {
			JsonObject updateObject = SoilePermissionProvider.getPermissionUpdate(alterationFlag, permissions, options.getPermissionField());
			client.findOneAndUpdateWithOptions(options.getCollectionName(), queryObject, updateObject, new FindOptions(), new UpdateOptions().setUpsert(false))
			.onComplete(res -> {
				LOGGER.debug(res);
				resultHandler.handle(res.mapEmpty());

			});	
		}
		catch(CannotUpdateMultipleException e)
		{
			resultHandler.handle(Future.failedFuture(e));
		}
		return this;
	}	

	
	/**
	 * Change roles or permissions indicating the correct field of the database. 
	 * @param username the id to add the roles/permissions for.
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param targetElement the target of the permission change
	 * @param newType the new type of permission
	 * @param alterationFlag the flag of the change (add/remove/set) 
	 * @param resultHandler the {@link Handler} of the result
	 * @return this object
	 */
	public SoileUserManager changePermissions(String username, MongoAuthorizationOptions options, String targetElement, PermissionType newType, PermissionChange alterationFlag, Handler<AsyncResult<Void>> resultHandler)
	{

		LOGGER.debug("Trying to update");
		JsonObject queryObject = new JsonObject().put(options.getUsernameField(), username);
		JsonObject updateObject = SoilePermissionProvider.getPermissionUpdate(targetElement, alterationFlag, newType, options.getPermissionField());
		if(!alterationFlag.equals(PermissionChange.Update))
		{
			// These are all atomic operations. 
			client.findOneAndUpdateWithOptions(options.getCollectionName(), queryObject, updateObject, new FindOptions(), new UpdateOptions().setUpsert(false))
			.onComplete(res -> {
				LOGGER.debug(res);
				resultHandler.handle(res.mapEmpty());
	
			});
		}
		else
		{
			// This is a two step operation. So we need to separate the updates.
			JsonObject pullUpdate = new JsonObject().put("$pullAll", updateObject.remove("$pullAll"));
			JsonObject pushUpdate = updateObject; // only push is left over.
			List<BulkOperation> pullAndPut = new LinkedList<>();
			BulkOperation pullOp = BulkOperation.createUpdate(queryObject, pullUpdate);
			BulkOperation pushOp = BulkOperation.createUpdate(queryObject, pushUpdate);
			pullAndPut.add(pullOp);
			pullAndPut.add(pushOp);		
			client.bulkWrite(authnOptions.getCollectionName(), pullAndPut).onComplete(result -> {
				resultHandler.handle(result.mapEmpty());
			});
		}		
		return this;
	}		
	
	/**
	 * Update the permissions for a given user, replacing the old ones by the new ones.
	 * @param username the id of the user to remove permissions
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param permissions the the permissions to remove
	 * @param resultHandler a result handler to handle the results.
	 * @return this object
	 */
	public SoileUserManager updatePermissions(String username, MongoAuthorizationOptions options, JsonArray permissions, Handler<AsyncResult<Void>> resultHandler)
	{
		return changePermissions(username, options, permissions,PermissionChange.Set, resultHandler);		
	}		 


	/**
	 * Update the roles for a given user, replacing the old ones by the new ones.
	 * @param username the id of the user to remove permissions
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param role the new role
	 * @param resultHandler a result handler to handle the results.
	 * @return this object
	 */
	public SoileUserManager updateRole(String username, MongoAuthorizationOptions options, String role, Handler<AsyncResult<JsonObject>> resultHandler)
	{
		JsonObject queryObject = new JsonObject().put(options.getUsernameField(), username);
		try
		  {
			  Roles.valueOf(role);
		  }
		  catch(IllegalArgumentException e)
		  {
			  resultHandler.handle(Future.failedFuture(new InvalidRoleException(role)));
			  return this;
		  }
		JsonObject updateObject = new JsonObject().put("$set", new JsonObject().put(options.getRoleField(), new JsonArray().add(role)));
		UpdateOptions opts = new UpdateOptions().setUpsert(false);
		client.findOneAndUpdateWithOptions(options.getCollectionName(), queryObject, updateObject,new FindOptions(),opts)
		.onSuccess(res -> {
			LOGGER.debug("Found " + res + " for user " + username);
			if(res == null)
			{
				resultHandler.handle(Future.failedFuture(new UserDoesNotExistException(username)));
			}
			else
			{
				resultHandler.handle(Future.succeededFuture(res));
			}
		})
		.onFailure(err -> {
			resultHandler.handle(Future.failedFuture(err));
		});		
		return this;
	}
	/**
	 * Remove a permission for a specific user
	 * @param username the id of the user to remove permissions
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param permission the the permission to add (as a permission string)
	 * @param resultHandler a result handler to handle the results.
	 * @return this object 
	 */
	public SoileUserManager addPermission(String username, MongoAuthorizationOptions options, String permission, Handler<AsyncResult<Void>> resultHandler)
	{
		addPermissions(username, options, new JsonArray().add(permission), resultHandler);
		return this;
	}		

	/**
	 * Remove a permission for a specific user
	 * @param username the id of the user to remove permissions
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param permissions the the permissions to add
	 * @param resultHandler a result handler to handle the results.
	 * @return this object
	 */
	public SoileUserManager addPermissions(String username, MongoAuthorizationOptions options, JsonArray permissions, Handler<AsyncResult<Void>> resultHandler)
	{
		return this.changePermissions(username, options, permissions, PermissionChange.Add, resultHandler);

	}

	/**
	 * Remove a permission for a specific user
	 * @param username the id of the user to remove permissions
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param permission the the permission to remove
	 * @param resultHandler a result handler to handle the results.
	 * @return this object
	 */
	public SoileUserManager removePermission(String username, MongoAuthorizationOptions options, String permission, Handler<AsyncResult<Void>> resultHandler)
	{
		removePermissions(username,options, new JsonArray().add(permission), resultHandler);
		return this;
	}		

	/**
	 * Remove a permission for a specific user
	 * @param username the id of the user to remove permissions
	 * @param options The {@link MongoAuthorizationOptions} used (indicating field names)
	 * @param permissions the the permissions to remove
	 * @param resultHandler a result handler to handle the results.
	 * @return this object
	 */
	public SoileUserManager removePermissions(String username, MongoAuthorizationOptions options, JsonArray permissions, Handler<AsyncResult<Void>> resultHandler)
	{
		return this.changePermissions(username, options, permissions, PermissionChange.Remove, resultHandler);

	}	
	
	/**
	 * create a user with the given name and password
	 * @param username name of the user
	 * @param password password of the new user
	 * @param resultHandler the {@link Handler} handling the result (
	 * @return this {@link SoileUserManager}
	 */
	public SoileUserManager createUser(String username, String password, Handler<AsyncResult<String>> resultHandler) {
		if (username == null || password == null) {
			resultHandler.handle(Future.failedFuture("username or password are null"));			
			return this;
		}
		// This needs to be updated!
		// we have all required data to insert a user
		return createHashedUser(
				username,
				createPasswordHash(password),
				resultHandler
				);
	}


	/**
	 * Delete a user from the database
	 * @param username the username to delete
	 * @param resultHandler the handler handling the result
	 * @return this {@link SoileUserManager}
	 */	
	public SoileUserManager deleteUser(String username, Handler<AsyncResult<Void>> resultHandler)
	{
		resultHandler.handle(deleteUser(username));		
		return this;
	}
	/**
	 * Delete the user with the given name 
	 * @param username the username of the user to delete
	 * @return A {@link Future} that is successful if the user was deleted.
	 */
	public Future<Void> deleteUser(String username)
	{
		// CleanUp of the users data needs to be done elsewhere, and this needs to be done after the cleanup.
		Promise<Void> deletionPromise = Promise.promise();	
		client.removeDocuments(
				authnOptions.getCollectionName(),
				new JsonObject().
				put(authnOptions.getUsernameField(), username))
		.onSuccess(res -> { 						
			if( res.getRemovedCount() >= 1)
			{							
				deletionPromise.complete();					
			}
			else
			{
				deletionPromise.fail(new UserDoesNotExistException(username));						
			}				
		}).onFailure(err -> deletionPromise.fail(err));
		return deletionPromise.future();
	}

	/**
	 * Retrieve the participant ID of this user in the given project. 
	 * @param username the username to get the partiicpant for
	 * @param project the project for which to get the {@link Participant}s ID for this given username
	 * @return A Future of the ID of the participant for this user in the project.If the user has no participant in the project, the Future will be <code>null</code>. 
	 */
	public Future<String> getParticipantIDForUserInStudy(String username, String project)
	{
		Promise<String> participantPromise = Promise.promise();
		client.findOne(authnOptions.getCollectionName(),new JsonObject().put(authnOptions.getUsernameField(), username), new JsonObject().put(SoileConfigLoader.getUserdbField("participantField"),1 ))
		.onSuccess(participantJson -> {
			JsonArray participantInfo = participantJson.getJsonArray(SoileConfigLoader.getUserdbField("participantField"), new JsonArray());
			String participantID = null;
			for(int i = 0; i < participantInfo.size(); i++)
			{
				// TODO: Use an aggregation to retrieve this (that's probably faster).
				if(participantInfo.getJsonObject(i).getString("UUID").equals(project))
				{
					participantID = participantInfo.getJsonObject(i).getString("participantID");
					break;
				}

			}
			participantPromise.complete(participantID);
		})
		.onFailure(err -> participantPromise.fail(err));

		return participantPromise.future();																				  													 																																					
	}

	/**
	 * Get all project/participant ID combinations for the given user.
	 * @param username The user for which data is requested.
	 * @return A {@link Future} of a {@link JsonArray} containing objects  with { UUID: {@literal <}participantID : {@literal <}IDofParticipantInStudy> }. 
	 */
	public Future<JsonArray> getParticipantInfoForUser(String username)
	{
		Promise<JsonArray> participantInfoPromise = Promise.promise();
		client.findOne(authnOptions.getCollectionName(),new JsonObject().put(authnOptions.getUsernameField(), username), new JsonObject().put(SoileConfigLoader.getUserdbField("participantField"),1 ))
		.onSuccess(participantJson -> {
			JsonArray participantInfo = participantJson.getJsonArray(SoileConfigLoader.getUserdbField("participantField"), new JsonArray());
			participantInfoPromise.complete(participantInfo);
		})
		.onFailure(err -> participantInfoPromise.fail(err));

		return participantInfoPromise.future();																																				
	}

	/**
	 * Assign the given participantID to the user in the Study indicated by the studyID.
	 * @param username The username for whom to assign a participantID
	 * @param studyID the study in which to assign the id
	 * @param participantID the participantID.
	 * @return A successfull future if this call worked.
	 */
	public Future<Void> makeUserParticipantInStudy(String username, String studyID, String participantID)
	{
		JsonObject query = new JsonObject().put(authnOptions.getUsernameField(), username);
		JsonObject pullUpdate = new JsonObject().put("$pull", new JsonObject()
				.put(SoileConfigLoader.getUserdbField("participantField"), new JsonObject()
						.put("UUID", new JsonObject()
								.put("$eq", studyID))));
		JsonObject pushUpdate = new JsonObject().put("$push", new JsonObject()
				.put(SoileConfigLoader.getUserdbField("participantField"), new JsonObject().put("UUID", studyID).put("participantID", participantID)));
		List<BulkOperation> pullAndPut = new LinkedList<>();
		BulkOperation pullOp = BulkOperation.createUpdate(query, pullUpdate);
		BulkOperation pushOp = BulkOperation.createUpdate(query, pushUpdate);
		pullAndPut.add(pullOp);
		pullAndPut.add(pushOp);		
		return client.bulkWrite(authnOptions.getCollectionName(), pullAndPut).mapEmpty();
	}
	
	/**
	 * REmove a user as participant in a specified study.
	 * @param username The username to change
	 * @param studyID the study ID to remove from
	 * @param participantID the participantID to delete
	 * @return a successful {@link Future} if the user was removed from the study
	 */
	public Future<Void> removeUserAsParticipant(String username, String studyID, String participantID)
	{
		JsonObject query = new JsonObject().put(authnOptions.getUsernameField(), username);
		JsonObject pullUpdate = new JsonObject().put("$pull", new JsonObject()
				.put(SoileConfigLoader.getUserdbField("participantField"), new JsonObject()
						.put("participantID", new JsonObject()
								.put("$eq", participantID))
						.put("UUID", new JsonObject()
								.put("$eq", studyID))));
		return client.findOneAndUpdate(authnOptions.getCollectionName(), query, pullUpdate).mapEmpty();
	}
	

	/**
	 * Create a user (hashed password) 
	 * @param username the username of the user to create
	 * @param hash the password hash to use
	 * @param resultHandler the Handler that should handle the id of the mongodb object created for the user
	 * @return this {@link SoileUserManager}
	 */
	public SoileUserManager createHashedUser(String username, String hash, Handler<AsyncResult<String>> resultHandler) {
		if (username == null || hash == null) {
			resultHandler.handle(Future.failedFuture("username or password hash are null"));
			return this;
		}
		if (username.contains("@") || username.contains(" "))
		{
			resultHandler.handle(Future.failedFuture("@ not allowed in usernames"));	
		}
		client.find(
				authnOptions.getCollectionName(),
				new JsonObject()
				.put(authnOptions.getUsernameField(), username)
				).andThen(
				res -> {
					if(res.succeeded())
					{
						if(res.result().size() > 0)
						{							
							resultHandler.handle(Future.failedFuture(new UserAlreadyExistingException(username)));
							return;
						}
						else {
							LOGGER.debug("Created user with username: " + username);
							client.save(
									authnOptions.getCollectionName(),
									getDefaultFields()
									.put(authnOptions.getUsernameField(), username)
									.put(authnOptions.getPasswordField(), hash)									
									).andThen(									
									resultHandler
									);							
						}
					}
					else
					{
						resultHandler.handle(Future.failedFuture("Unable to access user database"));
						return;
					}
				});

		return this;
	}



	/**
	 * Remove a given SessionID from a given username, invalidating that session (e.g. called by logout) 
	 * @param username The username for which to deactivate the session
	 * @param sessionID the sessionID to deactivate
	 * @param handler A handler that handles the resulting {@link MongoClientUpdateResult}
	 * @return this, for fluent use.
	 */
	public SoileUserManager removeUserSession(String username, String sessionID, Handler<AsyncResult<MongoClientUpdateResult>> handler)	
	{
		if (username == null  || sessionID == null) {
			handler.handle(Future.failedFuture("Username or session not given"));
			return this;
		}
		// We hash this vs timing attacks.
		String hashedSessionID = strategy.hash(hashingAlgorithm,
				null,
				SoileConfigLoader.getServerProperty("sessionStoreSecret"),
				sessionID); 
		JsonObject query = new JsonObject()
				.put(authnOptions.getUsernameField(), username); 
		client.find(
				authnOptions.getCollectionName(),
				query)
				.andThen(
				res -> {
					if(res.succeeded())
					{
						if(res.result().size() == 1)
						{
							// Adding the current session ID as valid session for the user.
							JsonObject validSessions = res.result()
									.get(0)
									.getJsonObject(SoileConfigLoader.getUserdbField("storedSessions"));
							if(validSessions != null)
							{	//if it's not initialized.
								//check for sessions that are too old;
								for(String session : validSessions.fieldNames())
								{								
									Long ctime = validSessions.getLong(session);
									//if this session is still valid keep it.
									if(System.currentTimeMillis() - ctime >SoileConfigLoader.getSessionLongProperty("maxTime"))
									{
										validSessions.remove(session);
									}
									if(session.equals(hashedSessionID))
									{
										validSessions.remove(session);
									}
								}
							}
							else
							{
								validSessions = new JsonObject();
							}
							client.updateCollection(authnOptions.getCollectionName(),
									query,
									new JsonObject()
									.put("$set", new JsonObject()
											.put(authnOptions.getUsernameField(), username)
											.put(SoileConfigLoader.getUserdbField("storedSessions"), validSessions))
									).andThen(handler);							
							return;
						}
						else {		
							if(res.result().size() == 0)
							{
								handler.handle(Future.failedFuture(new UserDoesNotExistException(username)));	
							}
							else
							{
								handler.handle(Future.failedFuture(new DuplicateUserEntryInDBException(username)));
							}
							return;	
						}



					}
					else
					{
						handler.handle(Future.failedFuture(new Exception("Unable to access user database")));
						return;
					}
				});
		return this;
	}


	/**
	 * Add a session to a user that can be used for re-authentication.
	 * @param username The username for which to add a session
	 * @param sessionID the ID of the session to add 
	 * @param handler the handler to handle the resulting {@link MongoClientUpdateResult}
	 * @return this {@link SoileUserManager} for fluent use
	 */
	public SoileUserManager addUserSession(String username, String sessionID, Handler<AsyncResult<MongoClientUpdateResult>> handler)	
	{
		if (username == null  || sessionID == null) {
			handler.handle(Future.failedFuture("Username or session not given"));
			return this;
		}
		// We hash this vs timing attacks.
		String hashedSessionID = strategy.hash(hashingAlgorithm,
				null,
				SoileConfigLoader.getServerProperty("sessionStoreSecret"),
				sessionID);
		LOGGER.debug("sessionID is :" + sessionID);
		LOGGER.debug("hashed ID is :" + hashedSessionID);
		JsonObject query = new JsonObject()
				.put(authnOptions.getUsernameField(), username); 
		client.find(
				authnOptions.getCollectionName(),
				query)
				.andThen( res -> {
					if(res.succeeded())
					{
						if(res.result().size() == 1)
						{
							LOGGER.debug("Found one user object, trying to update sessions");
							// Adding the current session ID as valid session for the user.
							JsonObject validSessions = res.result()
									.get(0)
									.getJsonObject(SoileConfigLoader.getUserdbField("storedSessions"));
							if(validSessions != null)
							{	//if it's initialized.
								//check for sessions that are too old;
								for(String session : validSessions.fieldNames())
								{								
									Long ctime = validSessions.getLong(session);
									//if this session is still valid keep it.
									if(System.currentTimeMillis() - ctime > SoileConfigLoader.getSessionLongProperty("maxTime"))
									{
										validSessions.remove(session);
									}
								}
							}
							else
							{
								validSessions = new JsonObject();
							}

							validSessions.put(hashedSessionID, System.currentTimeMillis());
							LOGGER.debug("Trying to add the following sessions:\n" + validSessions.encodePrettily());
							client.updateCollection(authnOptions.getCollectionName(),
									query,
									new JsonObject()
									.put("$set", new JsonObject()
											.put(authnOptions.getUsernameField(), username)
											.put(SoileConfigLoader.getUserdbField("storedSessions"), validSessions))
									).andThen(handler);							
							return;
						}
						else {		
							if(res.result().size() == 0)
							{
								handler.handle(Future.failedFuture(new UserDoesNotExistException(username)));	
							}
							else
							{
								handler.handle(Future.failedFuture(new DuplicateUserEntryInDBException(username)));
							}
							return;	
						}



					}
					else
					{
						handler.handle(Future.failedFuture(new Exception("Unable to access user database")));
						return;
					}
				});
		return this;
	}

	/**
	 * Test whether a given session is (still) valid for a given user
	 * @param username the username of the user
	 * @param sessionID the sessionID to check
	 * @param handler the handler to handle the {@link Boolean} {@link Future}
	 * @return this {@link SoileUserManager} for fluent use
	 */
	public SoileUserManager isSessionValid(String username, String sessionID, Handler<AsyncResult<Boolean>> handler)	
	{
		if (username == null  || sessionID == null) {
			handler.handle(Future.failedFuture("Username or session not given"));
			return this;
		}
		String hashedSessionID = strategy.hash(hashingAlgorithm,
				null,
				SoileConfigLoader.getServerProperty("sessionStoreSecret"),
				sessionID);
		LOGGER.debug("sessionID is: " + sessionID);
		LOGGER.debug("hashed ID is: " + hashedSessionID);
		JsonObject query = new JsonObject()
				.put(authnOptions.getUsernameField(), username); 
		client.find(
				authnOptions.getCollectionName(),
				query).andThen(
				res -> {
					if(res.succeeded())
					{
						if(res.result().size() == 1)
						{
							LOGGER.debug(res.result().get(0).encodePrettily());
							// Getting the currently stored session ids
							JsonObject storedSessions = res.result()
									.get(0)
									.getJsonObject(SoileConfigLoader.getUserdbField("storedSessions"));
							Long startTime = storedSessions.getLong(hashedSessionID);
							// if this is not present (i.e. the result is null) then it's not a stored ID.					
							LOGGER.debug("Current Time: " + System.currentTimeMillis() +  "; StartTime was: " + startTime + "; Max Age is: " + SoileConfigLoader.getSessionLongProperty("maxTime"));
							if(startTime != null && (System.currentTimeMillis() - startTime < SoileConfigLoader.getSessionLongProperty("maxTime")))
							{
								LOGGER.debug("Session validated Successfully");
								handler.handle(Future.succeededFuture(true));
							}							
							else
							{
								LOGGER.debug("Session validated successfully, but no longer valid.");
								handler.handle(Future.succeededFuture(false));	
							}
						}
						else {		
							if(res.result().size() == 0)
							{
								handler.handle(Future.failedFuture(new UserDoesNotExistException(username)));	
							}
							else
							{
								handler.handle(Future.failedFuture(new DuplicateUserEntryInDBException(username)));
							}
							return;	
						}



					}
					else
					{
						handler.handle(Future.failedFuture(new Exception("Unable to access user database")));
						return;
					}
				});
		return this;
	}

	@Override
	public Future<String> createUser(String username, String password) {
		Promise<String> promise = Promise.promise();
		createUser(username, password)
		.onFailure(fail -> promise.fail(fail))
		.onSuccess(res -> promise.complete(res));
		return promise.future();
	}

	@Override
	public Future<String> createHashedUser(String username, String hash) {
		Promise<String> promise = Promise.promise();
		createHashedUser(username, hash)
		.onFailure(fail -> promise.fail(fail))
		.onSuccess(res -> promise.complete(res));
		return promise.future();
	}

	@Override
	public Future<String> createUserRolesAndPermissions(String user, List<String> roles, List<String> permissions) {
		Promise<String> promise = Promise.promise();
		createUserRolesAndPermissions(user, roles,permissions)
		.onFailure(fail -> promise.fail(fail))
		.onSuccess(res -> promise.complete(res));
		return promise.future();
	}

	/**
	 * Set the user information for a user (fullname, email, role) 
	 * @param username the user for which to change the information
	 * @param command the new values ( currently only "email", "role" and "fullname" fields are supported.
	 * @return A Future that indicates whether this operation was a success
	 */
	public Future<JsonObject> setUserInfo(String username, JsonObject command) {
		Promise<JsonObject> userUpdatedPromise = Promise.promise();
		JsonObject updates = new JsonObject();
		JsonObject updateCommand = new JsonObject().put("$set", updates);
		LOGGER.debug("Setting User information:" + command.encode());
		for(String key : command.fieldNames())
		{			
			String target = "";
			switch(key)
			{
			case "email": target = SoileConfigLoader.getUserdbField("userEmailField");
						  if(!UserUtils.isValidEmail(command.getString(key)))
						  {
							  LOGGER.debug("Invalid email: " + command.getString(key));
							  return Future.failedFuture(new InvalidEmailAddress(target));
						  }
						  break;
			case "role":  target = authzOptions.getRoleField();
							  try
							  {
								  Roles.valueOf(command.getString(key));
								  if(command.getValue("role") instanceof String)
								  {
									  command.put("role", new JsonArray().add(command.getValue(key)));
								  }
							  }
							  catch(IllegalArgumentException e)
							  {
								  return Future.failedFuture(new InvalidRoleException(command.getString(key)));  
							  }							  
			  break;
			case "fullname": target = SoileConfigLoader.getUserdbField("userFullNameField");
			  break;			
			}
			if(!target.equals(""))
			{
				updates.put(target, command.getValue(key));
			}
		}
		LOGGER.debug("Updates are:" + updates.encode());
		if(updates.size() == 0)
		{
			// nothing to do... 
			return Future.succeededFuture();
		}			
		JsonObject query = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), username);	
		isEmailInUse(command.getString("email"),username)
		.onSuccess(inUse -> {
			if(inUse)
			{
				userUpdatedPromise.fail(new EmailAlreadyInUseException(command.getString("email")));
			}
			else
			{
				LOGGER.debug("Updating");
				UpdateOptions opts = new UpdateOptions().setUpsert(false).setReturningNewDocument(true);
				FindOptions findOpts = new FindOptions().setFields(new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), 1)
						.put(SoileConfigLoader.getUserdbField("userEmailField"), 1)
						.put(SoileConfigLoader.getUserdbField("userRolesField"), 1)
						.put(SoileConfigLoader.getUserdbField("userFullNameField"), 1)
						.put("_id", 0));
				client.findOneAndUpdateWithOptions(authnOptions.getCollectionName(), query, updateCommand,findOpts, opts)				
				.onSuccess(res -> {
					if(res == null)
					{
						userUpdatedPromise.fail(new UserDoesNotExistException(username));
					}
					else
					{
						userUpdatedPromise.complete(res);
					}
				})
				.onFailure(err -> userUpdatedPromise.fail(err));
			}
		})
		.onFailure(err -> userUpdatedPromise.fail(err));		
		return userUpdatedPromise.future();
	}

	/**
	 * Get the user information for a user (fullname, email, role) 
	 * @param username the user for which to change the information
	 * @return A {@link Future} of the {@link JsonObject} containing the data
	 */
	public Future<JsonObject> getUserInfo(String username) {
		JsonObject query = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), username);
		JsonObject fields = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), 1)
											.put(SoileConfigLoader.getUserdbField("userEmailField"), 1)
											.put(SoileConfigLoader.getUserdbField("userRolesField"), 1)
											.put(SoileConfigLoader.getUserdbField("userFullNameField"), 1)
											.put("_id", 0);
		return client.findOne(authnOptions.getCollectionName(), query, fields);
	}

	/**
	 * Set the user password 
	 * @param username the user for which to change the information
	 * @param password the new password for the user.
	 * @return A Future that indicates whether this operation was a success
	 */
	public Future<Void> setPassword(String username, String password) {
		JsonObject query = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), username);
		JsonObject update = new JsonObject().put("$set", new JsonObject().put(authnOptions.getPasswordField(), createPasswordHash(password)));
		return client.findOneAndUpdate(authnOptions.getCollectionName(), query, update).compose(res -> {
			if(res == null)
			{				 
				return Future.failedFuture(new UserDoesNotExistException(username));
			}
			else
			{
				return Future.succeededFuture();
			}
		});
	}
	
	/**
	 * Remove Participants in specific studies from users. This is mainly necessary if a study got reset.
	 * This is primarily an update method for the user db, in order to stay in sync with the participant dbs. 
	 * @param studyID the ID of the study to remove the participant form 
	 * @param participantID the participant to remove
	 * @return A Future of a JsonOBject with the permissions.
	 */
	public Future<Void> removeParticipantInStudyFromUsers(String studyID, String participantID) {
		JsonObject query = new JsonObject().put(SoileConfigLoader.getUserdbField("participantField"),
												new JsonObject().put("$in", new JsonArray().add( new JsonObject().put("UUID", studyID)
																	.put("participantID", participantID))));
		JsonObject pullObject = new JsonObject().put("$pull", 
													 new JsonObject().put(SoileConfigLoader.getUserdbField("participantField"),
															 			  new JsonObject().put("UUID", studyID)
																                          .put("participantID", participantID)));				
		LOGGER.debug("StudyID: " + studyID + "// ParticipantID: " + participantID);
		return client.find(authnOptions.getCollectionName(), query).compose(found -> {
			if(found.size() > 1)
			{
				LOGGER.error("Found more than one user with this participant");
				return Future.failedFuture(new DuplicateUserEntryInDBException("Duplicate Participant in users"));
			}
			if(found.size() == 0) 				
			{
				// this is ok. If no matching user exists, the participant doesn't need to be removed from any user, although this is an odd occurence...
				LOGGER.warn("User not found");
				return Future.succeededFuture();
			}
			return client.updateCollection(authnOptions.getCollectionName(), query, pullObject).mapEmpty();
		});
	}
	
	/**
	 * Get the Information about a users Permissions/Access
	 * @param username The username for which to get the permissions
	 * @return A Future of a JsonOBject with the permissions.
	 */
	public Future<JsonObject> getUserAccessInfo(String username) {
		JsonObject query = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), username);
		JsonObject fields = new JsonObject().put(SoileConfigLoader.getUserdbField("userRolesField"), 1)
											.put(SoileConfigLoader.getUserdbField("experimentPermissionsField"), 1)
											.put(SoileConfigLoader.getUserdbField("studyPermissionsField"), 1)
											.put(SoileConfigLoader.getUserdbField("projectPermissionsField"), 1)
											.put(SoileConfigLoader.getUserdbField("taskPermissionsField"), 1)
											.put("_id", 0);
		return client.findOne(authnOptions.getCollectionName(),query,fields);
	}
	
	/**
	 * Get users with access to a specific Study
	 * @param studyID the Study for which to retrieve users 
	 * @return A {@link JsonArray} of objects with "user" and "access" fields, where access is one of READ / READ_WRITE / FULL and user is a username  
	 */
	public Future<JsonArray> getUserWithAccessToStudy(String studyID) {
		JsonObject query = new JsonObject().put(SoileConfigLoader.getUserdbField("studyPermissionsField"),
												new JsonObject().put("$elemMatch", 
																	 new JsonObject().put("$regex", SoilePermissionProvider.buildPermissionQuery(studyID, PermissionType.READ))));
		JsonObject fields = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), 1)											
											.put(SoileConfigLoader.getUserdbField("studyPermissionsField"), new JsonObject().put("$elemMatch", 
													 new JsonObject().put("$regex", SoilePermissionProvider.buildPermissionQuery(studyID, PermissionType.READ))))																						
											.put("_id", 0);
		return client.findWithOptions(authnOptions.getCollectionName(),query,new FindOptions().setFields(fields)).map(targets -> {
			JsonArray result = new JsonArray();
			for(JsonObject res : targets)
			{
				JsonArray existingpermissions = res.getJsonArray(SoileConfigLoader.getUserdbField("studyPermissionsField"));
				JsonObject user = new JsonObject().put("user", res.getString(SoileConfigLoader.getUserdbField("usernameField")));
				List<PermissionType> permissions = new LinkedList<>();
				for(int i = 0; i < existingpermissions.size(); i++)
				{
					permissions.add(PermissionType.valueOf(SoilePermissionProvider.getTypeFromPermission(existingpermissions.getString(i))));
				}
				user.put("access", SoilePermissionProvider.getMaxPermission(permissions));
				result.add(user);
			}
			
			return result;
		});
	}
	
	
	private JsonObject getDefaultFields()
	{
		return new JsonObject().put(SoileConfigLoader.getUserdbField("userRolesField"), new JsonArray().add(Roles.Participant.toString()))
								.put(SoileConfigLoader.getUserdbField("experimentPermissionsField"), new JsonArray())
								.put(SoileConfigLoader.getUserdbField("studyPermissionsField"), new JsonArray())
								.put(SoileConfigLoader.getUserdbField("projectPermissionsField"), new JsonArray())
								.put(SoileConfigLoader.getUserdbField("taskPermissionsField"), new JsonArray())
								.put(SoileConfigLoader.getUserdbField("userEmailField"), "")
								.put(SoileConfigLoader.getUserdbField("userFullNameField"), "")
								.put(SoileConfigLoader.getUserdbField("participantField"), new JsonArray())
								.put(SoileConfigLoader.getUserdbField("storedSessions"), new JsonObject());		 		
	}
	
	private String createPasswordHash(String password)
	{
		final byte[] salt = new byte[32];
		random.nextBytes(salt);
		return strategy.hash(hashingAlgorithm,
						null,
						base64Encode(salt),
						password);
	}
}
