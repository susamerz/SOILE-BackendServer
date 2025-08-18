package fi.abo.kogni.soile2.http_server.verticles;

import java.net.HttpURLConnection;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.PermissionType;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.TargetElementType;
import fi.abo.kogni.soile2.http_server.auth.SoilePermissionProvider;
import fi.abo.kogni.soile2.http_server.userManagement.SoileUserManager;
import fi.abo.kogni.soile2.http_server.userManagement.SoileUserManager.PermissionChange;
import fi.abo.kogni.soile2.http_server.userManagement.UserUtils;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.EmailAlreadyInUseException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.InvalidEmailAddress;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.InvalidPermissionTypeException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.InvalidRoleException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.UserAlreadyExistingException;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.UserDoesNotExistException;
import fi.abo.kogni.soile2.projecthandling.exceptions.ObjectDoesNotExist;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.mongo.MongoAuthorizationOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.handler.HttpException;
/**
 * This class provides functionality for the user management of a soile project. it processes messages addressed to 
 * the {UserManagement_prefix} and a command suffix.
 * The messages need to contain JsonObjects of the following structure:
 * { 
 *   {@literal <}usernameField> : "username",
 *   {@literal <}passwordField> : "password",
 *   {@literal <}userEmailField> : "email", # optional
 *   {@literal <}userFullNameField> : "fullname", # optional
 *   {@literal <}userRolesField> : [ "role1" , "role2" ] # optional
 *   {@literal <}userPermissionsField> : [ "permission1" , "permission2" ] # optional
 *   {@literal <}userTypeField> : "type"
 *   } 
 * Passwords need to be transferred using the systems hashing algorithm
 * @author thomas
 *
 */
public class SoileUserManagementVerticle extends SoileBaseVerticle {

	SoileUserManager userManager;	
	MongoClient mongo;
	private static final Logger LOGGER = LogManager.getLogger(SoileUserManagementVerticle.class);
	private List<MessageConsumer> consumers;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		LOGGER.debug("Starting UserManagementVerticle with ID: " + deploymentID()  );
		mongo = MongoClient.createShared(vertx, SoileConfigLoader.getMongoCfg(), "USER_MANAGER_MONGO");

		setupConfig(SoileConfigLoader.USERMGR_CFG);		
		userManager = new SoileUserManager(mongo);		
		consumers = new LinkedList<>();
		setupChannels();
		LOGGER.debug("User Management Verticle Started");
		//LOGGER.debug("\n\nUser Management Verticle Started\n\n");

		userManager.setupDB()
		.onSuccess(suceeded -> {
			startPromise.complete();	
		})
		.onFailure(err -> startPromise.fail(err));

	}

	/**
	 * Set up the channels this Manager is listening to.
	 */
	void setupChannels()
	{				
		LOGGER.debug("Setting up channels");

		consumers.add(vertx.eventBus().consumer("soile.umanager.addUser", this::addUser));
		consumers.add(vertx.eventBus().consumer("soile.umanager.addUserWithEmail", this::addUserWithEmail));
		consumers.add(vertx.eventBus().consumer("soile.umanager.removeUser", this::removeUser));		
		consumers.add(vertx.eventBus().consumer("soile.umanager.permissionOrRoleChange", this::permissionOrRoleChange));		
		//consumers.add(vertx.eventBus().consumer("soile.umanager.setUserFullNameAndEmail", this::setUserFullNameAndEmail));
		//consumers.add(vertx.eventBus().consumer("soile.umanager.getUserData", this::getUserData));
		//LOGGER.debug("Reistering eventbus consumer for:" + "soile.umanager.checkUserSessionValid"); 
		consumers.add(vertx.eventBus().consumer("soile.umanager.checkUserSessionValid", this::isSessionValid));
		consumers.add(vertx.eventBus().consumer("soile.umanager.addSession", this::addValidSession));
		consumers.add(vertx.eventBus().consumer("soile.umanager.removeSession", this::invalidateSession));		
		consumers.add(vertx.eventBus().consumer("soile.umanager.makeUserParticipantInStudy", this::makeUserParticipantInStudy));		
		consumers.add(vertx.eventBus().consumer("soile.umanager.removeParticipantFromStudy", this::removeUserFromStudy));
		consumers.add(vertx.eventBus().consumer("soile.umanager.getParticipantForUserInStudy", this::getParticipantForUser));
		consumers.add(vertx.eventBus().consumer("soile.umanager.getParticipantsForUser", this::getParticipantsForUser));
		consumers.add(vertx.eventBus().consumer("soile.umanager.listUsers", this::listUsers));
		consumers.add(vertx.eventBus().consumer("soile.umanager.setUserInfo", this::setUserInfo));
		consumers.add(vertx.eventBus().consumer("soile.umanager.getUserInfo", this::getUserInfo));
		consumers.add(vertx.eventBus().consumer("soile.umanager.getAccessRequest", this::getUserAccessInfo));
		consumers.add(vertx.eventBus().consumer("soile.umanager.setPassword", this::setPassword));
		consumers.add(vertx.eventBus().consumer("soile.umanager.getCollaboratorsforStudy", this::getCollaboratorsForStudy));
		consumers.add(vertx.eventBus().consumer("soile.umanager.removeParticipantInStudy", this::removeParticipantFromStudy));

	}


	@Override
	public void stop(Promise<Void> stopPromise)
	{
		List<Future<Void>> undeploymentFutures = new LinkedList<Future<Void>>();
		for(MessageConsumer consumer : consumers)
		{
			undeploymentFutures.add(consumer.unregister());
		}				
		Future.all(undeploymentFutures).mapEmpty().
		onSuccess(v -> {
			LOGGER.debug("Successfully undeployed SoileUserManager with id : " + deploymentID());			
			stopPromise.complete();
		})
		.onFailure(err -> stopPromise.fail(err));			
	}

	/**
	 * Add a session to a user
	 * {
	 *  {@literal <}usernameField> : username,
	 *  {@literal <}sessionID> : sessionID,
	 *  }
	 *  If the session was successfully added    
	 *  {
	 *  "Result" : "Success", 
	 *  }
	 *  and if there was an Error, the result will be:
	 *  {
	 *  "Result" : "Error",
	 *  "Reason" : "Explanation"
	 *  }
	 * @param msg
	 */
	void addValidSession(Message<Object> msg)
	{
		if (msg.body() instanceof JsonObject)
		{
			JsonObject command = (JsonObject)msg.body();			

			LOGGER.debug("Trying to save a session with the following data: \n" + command.encodePrettily());
			userManager.addUserSession(command.getString("username")
					,command.getString("sessionID")
					,res ->
			{
				if(res.succeeded())
				{
					LOGGER.debug("Adding Session suceeded");
					msg.reply(SoileCommUtils.successObject());
				}
				else
				{
					LOGGER.debug("Adding Session Failed: " + res.cause().getMessage());
					msg.fail(HttpURLConnection.HTTP_INTERNAL_ERROR, res.cause().getMessage());						
				}
			});
		}	
		else
		{
			msg.fail(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid Request");
		}
	}

	/**
	 * Remove a session from a user
	 * {
	 *  {@literal <}usernameField> : username,
	 *  {@literal <}sessionID> : sessionID,
	 *  }
	 *  If the session was successfully added    
	 *  {
	 *  "Result" : "Success", 
	 *  }
	 *  and if there was an Error, the result will be:
	 *  {
	 *  "Result" : "Error",
	 *  "Reason" : "Explanation"
	 *  }
	 * @param msg
	 */
	void invalidateSession(Message<Object> msg)
	{
		if (msg.body() instanceof JsonObject)
		{
			JsonObject command = (JsonObject)msg.body();			

			userManager.removeUserSession(command.getString("username")
					,command.getString("sessionID")
					,res ->
			{
				if(res.succeeded())
				{
					msg.reply(SoileCommUtils.successObject());
				}
				else
				{
					msg.fail(HttpURLConnection.HTTP_INTERNAL_ERROR, res.cause().getMessage());						
				}
			});
		}	
		else
		{
			msg.fail(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid Request");
		}
	}

	/**
	 * Test whether a given session is still valid for a given user.
	 * {
	 *  {@literal <}usernameField> : username,
	 *  {@literal <}sessionID> : sessionID,
	 *  }
	 *  I the session is valid the result will be a json with   
	 *  {
	 *  "Result" : "Success",
	 *  {@literal <}sessionIsValid> : true 
	 *  }
	 *  If the session is not valid, the result will be:
	 *  {
	 *  "Result" : "Success",
	 *  {@literal <}sessionIsValid> : False 
	 *  }
	 *  and if there was an Error, the result will be:
	 *  {
	 *  "Result" : "Error",
	 *  "Reason" : "Explanation"
	 *  }
	 * @param msg
	 */
	void isSessionValid(Message<Object> msg)
	{
		LOGGER.debug("Got a request for a session validation");
		if (msg.body() instanceof JsonObject)
		{
			JsonObject command = (JsonObject)msg.body();						
			LOGGER.debug("Checking, whether a session is valid:" + command.encodePrettily());
			userManager.isSessionValid(command.getString("username")
					,command.getString("sessionID")
					,res ->
			{
				if(res.succeeded())
				{
					if(res.result())
					{		
						LOGGER.debug("Session Valid. Replying accordingly");
						msg.reply(SoileCommUtils.successObject());
					}
					else
					{
						LOGGER.debug("Session no longer valid.");
						msg.fail(HttpURLConnection.HTTP_UNAUTHORIZED, "Session not valid");
					}
				}
				else
				{
					LOGGER.error(res.cause().getMessage() ) ;
					msg.fail(HttpURLConnection.HTTP_INTERNAL_ERROR, "Session could not be validated " +  res.cause().getMessage());						
				}
			});
		}	
		else
		{

			msg.fail(HttpURLConnection.HTTP_INTERNAL_ERROR, "Request Invalid" );
		}
	}

	/**
	 * Add a user with given email address
	 * {
	 *  {@literal <}usernameField> : username,
	 *  {@literal <}userEmailField> : emailAddress,
	 *  }
	 *  If the user was added successfully    
	 *  {
	 *  "Result" : "Success", 
	 *  }
	 *  and if there was an Error, the result will be:
	 *  {
	 *  "Result" : "Error",
	 *  "Reason" : "Explanation"
	 *  }
	 * @param msg
	 */
	void addUserWithEmail(Message<JsonObject> msg)
	{
		JsonObject command = msg.body();			
		String email = command.getString("email");
		if(!UserUtils.isValidEmail(email))
		{
			msg.fail(400, "Invalid Email address");
			return;
		}
		
		// we allow an empty role field, which will translate to a participant signing up. 
		if(command.containsKey("role"))
		{
			if(command.getString("role").equals(""))
			{
				command.put("role", Roles.Participant.toString());
			}
			
		}
		LOGGER.debug("Adding user with name:" + command.getString("username"));
		String userName = command.getString("username");
		//LOGGER.debug("Verticle: Creating user");
		userManager.createUser(userName,
				command.getString("password"))
		.onSuccess(created -> {
			LOGGER.debug("User with name:" + command.getString("username") + " added. Trying to set email address");
			userManager.setUserInfo(userName, command)
			.onSuccess(set -> {
				LOGGER.debug("Email for user " + command.getString("username") +  " set to " + email);
				msg.reply(SoileCommUtils.successObject());	
			})
			.onFailure(err -> {
				LOGGER.debug(err);				
				userManager.deleteUser(userName)				
				.onSuccess(res -> {
					LOGGER.debug("Removed user " + command.getString("username"));
					handleError(err, msg);	
				})
				.onFailure(crit -> {
					LOGGER.error("Created a user but couldn't remove it after email addition failed. Username: " + command.getString(userName));
					handleError(crit, msg);	
				});

			});
		})
		.onFailure(err -> {
			handleError(err, msg);	
		});
	}

	/**
	 * Add a User to the database. he message body must be a jsonObject with at least the following fields:
	 * {
	 *  {@literal <}usernameField> : username,
	 *  {@literal <}userTypeField> : userType,
	 *  {@literal <}userPasswordField> : userPassword,
	 *  {@literal <}userFullNameField> : Full Name (optional)
	 *  {@literal <}userEmailField> : Full Name (optional)
	 *  }
	 * @param msg
	 */
	void addUser(Message<Object> msg)
	{		
		//LOGGER.debug("Getting a user Creation request");
		//make sure we actually get the right thing
		if (msg.body() instanceof JsonObject)
		{
			JsonObject command = (JsonObject)msg.body();			

			//LOGGER.debug("Verticle: Creating user");
			userManager.createUser(command.getString("username"),
					command.getString("password"), id -> 
			{
				// do this only if the user was created Successfully
				if(id.succeeded()) {
					LOGGER.debug("User created successfully");
					LOGGER.debug("Username: " + command.getString(getDBField("usernameField"))
					+ " password: " + command.getString("password"));
					msg.reply(SoileCommUtils.successObject()); 			    					
				}
				else
				{
					handleError(id.cause(),msg);					
				}
			});
		}	
		else
		{
			msg.fail(HttpURLConnection.HTTP_BAD_REQUEST,"Invalid Request");
		}
	}


	/**
	 * Get the participant for the provided user in the given project. 
	 * @param msg
	 */
	void getParticipantForUser(Message<JsonObject> msg)
	{
		JsonObject command = msg.body();			
		userManager.getParticipantIDForUserInStudy(command.getString(getDBField("usernameField")), command.getString("studyID"))
		.onSuccess(res -> {
			msg.reply(SoileCommUtils.successObject().put("participantID", res));
		})
		.onFailure(err -> handleError(err, msg));						    							

	}

	/**
	 * Get the participant for the provided user in the given project. 
	 * @param msg
	 */
	void getParticipantsForUser(Message<JsonObject> msg)
	{
		JsonObject command = msg.body();			
		userManager.getParticipantInfoForUser(command.getString(getDBField("usernameField")))
		.onSuccess(res -> {
			msg.reply(SoileCommUtils.successObject().put("participantIDs", res));
		})
		.onFailure(err -> handleError(err, msg));						    							

	}


	/**
	 * Remove a user. The message body must contain the user name. 
	 * Note, this ONLY removes the user from the USER DB. It does NOT remove the data of the user, this has to be done elsewhere, and should be done beforehand. 
	 * @param msg
	 */
	void removeUser(Message<Object> msg)
	{
		//make sure we actually get the right thing
		if (msg.body() instanceof JsonObject)
		{
			JsonObject command = (JsonObject)msg.body();			

			userManager.deleteUser(command.getString(getDBField("usernameField")))
			.onSuccess(success -> {
				msg.reply(SoileCommUtils.successObject());
			})
			.onFailure(err -> handleError(err, msg));
		}	
		else
		{
			msg.fail(HttpURLConnection.HTTP_BAD_REQUEST,"Invalid Request");
		}
	}
	/**
	 * Change the permissions of a user. The command needs to have the following structure:
	 * {
	 * 		"{@literal <}usernameField>" : "UserName",	 * 		
	 * 		oneOf:
	 * 		 - command : "{@literal <}addCommand>"/"{@literal <}setCommand>"/"{@literal <}removeCommand>"
	 * 		   permissionsProperties
	 * 			{
	 * 			 elementType: "task"/"experiment"/"project"/"study"
	 * 			 permissionSettings:
	 * 				[
	 * 					{
	 * 						type: "READ"/"READ_WRITE"/"FULL"/"EXECUTE"
	 * 						target: idOftargetElement (e.g. abcdefg12345)
	 * 					}
	 * 				]    
	 * 			} 
	 * 		- role : "Admin"/"Researcher"/"Participant"  
	 * @param msg
	 */
	void permissionOrRoleChange(Message<JsonObject> msg)
	{	
		try {
			//make sure we actually get the right thing
			if (msg.body() instanceof JsonObject)
			{
				//first get the data from the body
				JsonObject command = (JsonObject) msg.body();
				String userName = command.getString("username");			
				String changeType = command.getString("command");			
				String role = command.getString("role");
				JsonObject permissions = command.getJsonObject("permissionsProperties");
				if(permissions != null && role != null) 
				{
					msg.fail(HttpURLConnection.HTTP_BAD_REQUEST, "Cannot change permission and role settings at the same time");
					return;
				}
				if(role != null)
				{

					userManager.updateRole(userName, SoileConfigLoader.getMongoAuthZOptions() , role, 
							res ->
					{
						if(res.succeeded())
						{
							msg.reply(SoileCommUtils.successObject());

						}
						else
						{
							LOGGER.error("Could not update permissions for request:\n" + command.encodePrettily() );
							handleError(res.cause(), msg);						
						}						
					});
					return;
				}
				if(permissions != null)
				{
					try
					{

																		
						if(permissions.getJsonArray("permissionSettings").size() == 1)
						{
							JsonObject permissionChange = permissions.getJsonArray("permissionSettings").getJsonObject(0);
							PermissionType permissionType = null;						
							try
							{
								permissionType = PermissionType.valueOf(permissionChange.getString("type"));								
							}
							catch(IllegalArgumentException | NullPointerException e)
							{
								throw new HttpException(400, permissionChange.getString("type") + " is not a valid type for permissions");
							}							
							
							//String username, MongoAuthorizationOptions options, String targetElement, PermissionType newType, PermissionChange alterationFlag, Handler<AsyncResult<JsonObject>> resultHandler
							userManager.changePermissions(userName, getOptionForType(permissions.getString("elementType")), 
														  permissionChange.getString("target"), permissionType, getChange(changeType), res -> {
							if(res.succeeded())
							{
								msg.reply(SoileCommUtils.successObject());
							}
							else
							{
								LOGGER.error("Could not update permissions for request:\n" + command.encodePrettily() );
								handleError(res.cause(), msg);	
							}	
						});
						}
						else
						{
							JsonArray alteredPermissions = convertPermissionsArray(permissions.getJsonArray("permissionSettings"));
							userManager.changePermissions(userName, getOptionForType(permissions.getString("elementType")), alteredPermissions, getChange(changeType), res -> {
								if(res.succeeded())
								{
									msg.reply(SoileCommUtils.successObject());
								}
								else
								{
									LOGGER.error("Could not update permissions for request:\n" + command.encodePrettily() );
									handleError(res.cause(), msg);	
								}	
							});	
						}
						return;
					}
					catch(HttpException e)
					{
						handleError(e, msg);					
						return;
					}
				}				
				handleError(new HttpException(400, "Need either Permissions or Roles to set"),msg);

			}
			else
			{
				msg.fail(HttpURLConnection.HTTP_BAD_REQUEST,"Invalid Request");
			}
		}
		catch(Exception e)
		{
			msg.fail(HttpURLConnection.HTTP_BAD_REQUEST,"Invalid Request");
		}
	}

	/**
	 * Get the participant for the provided user in the given project. 
	 * @param msg
	 */
	void getUserAccessInfo(Message<JsonObject> msg)
	{
		JsonObject command = msg.body();			
		String username = (String) command.remove("username");
		userManager.getUserAccessInfo(username)
		.onSuccess(res -> {
			if(res == null)
			{
				handleError(new ObjectDoesNotExist(username), msg);
				return;
			}
			// in case, we translate to the actual json Object
			// translate back into PermissionSettings
			JsonObject response = new JsonObject();			
			response.put("username",username)
			.put("role", res.getJsonArray(SoileConfigLoader.getUserdbField("userRolesField")).getString(0))
			.put("permissions", new JsonObject()
					.put("tasks", convertStringPermissions(res.getJsonArray(SoileConfigLoader.getUserdbField("taskPermissionsField"))))
					.put("projects", convertStringPermissions(res.getJsonArray(SoileConfigLoader.getUserdbField("projectPermissionsField"))))
					.put("experiments", convertStringPermissions(res.getJsonArray(SoileConfigLoader.getUserdbField("experimentPermissionsField"))))
					// TODO: Refactor to studies
					.put("instances", convertStringPermissions(res.getJsonArray(SoileConfigLoader.getUserdbField("studyPermissionsField"))))
					);
			msg.reply(SoileCommUtils.successObject().put(SoileCommUtils.DATAFIELD, response));
		})
		.onFailure(err -> handleError(err, msg));						    							
	}

	/**
	 * Get a list of Users based on some information
	 * {
	 *  "limit" : how many entries to return at most,
	 *  "skip" : how many results to skip,
	 *  "query" : a search string to use,
	 *  }
	 * @param msg
	 */
	void listUsers(Message<JsonObject> msg)
	{		
		//make sure we actually get the right thing			
		JsonObject command = msg.body();		
		userManager.getUserList(command.getInteger("skip"),command.getInteger("limit"), command.getString("query"), command.getString("type"), command.getBoolean("namesOnly", false))
		.onSuccess(list -> {					
			msg.reply(SoileCommUtils.successObject().put(SoileCommUtils.DATAFIELD, list));					
		})
		.onFailure(err -> 
		{
			LOGGER.error(err,err);
			msg.fail(400, "Error fetching Data");									
		});		
	}


	/**
	 * Get a list of Collaborators for a specified study
	 * {
	 *  "limit" : how many entries to return at most,
	 *  "skip" : how many results to skip,
	 *  "query" : a search string to use,
	 *  }
	 * @param msg
	 */
	void getCollaboratorsForStudy(Message<JsonObject> msg)
	{		
		//make sure we actually get the right thing			
		JsonObject command = msg.body();		
		userManager.getUserWithAccessToStudy(command.getString("studyID"))
		.onSuccess(list -> {					
			msg.reply(SoileCommUtils.successObject().put(SoileCommUtils.DATAFIELD, list));					
		})
		.onFailure(err -> 
		{
			LOGGER.error(err,err);
			msg.fail(400, "Error fetching Data");									
		});		
	}
	/**
	 * Remove a specified participant in a specified study from its user association.
	 * {
	 *  "participantID" : the participantID to remove,
	 *  "studyID" : the studyID of the participant,	 *  
	 *  }
	 * @param msg
	 */
	void removeParticipantFromStudy(Message<JsonObject> msg)
	{		
		//make sure we actually get the right thing			
		JsonObject command = msg.body();		
		userManager.removeParticipantInStudyFromUsers(command.getString("studyID"),command.getString("participantID"))
		.onSuccess(removed -> {					
			msg.reply(SoileCommUtils.successObject());					
		})
		.onFailure(err -> 
		{
			LOGGER.error(err,err);
			msg.fail(400, "Error fetching Data");									
		});		
	}
	
	/**
	 * Make a User participant in a project. The message must contain the username along with the studyID and the participantID. 
	 * @param msg
	 */
	void makeUserParticipantInStudy(Message<JsonObject> msg)
	{
		//make sure we actually get the right thing
		JsonObject command = msg.body();			

		userManager.makeUserParticipantInStudy(command.getString(getDBField("usernameField")), command.getString("studyID"), command.getString("participantID"))
		.onSuccess(res -> {
			msg.reply(SoileCommUtils.successObject());
		})
		.onFailure(err -> handleError(err, msg));						    							

	}
	
	
	/**
	 * Remove a User as participant from a study. The message must contain the username along with the studyID and the participantID. 
	 * @param msg
	 */
	void removeUserFromStudy(Message<JsonObject> msg)
	{
		//make sure we actually get the right thing
		JsonObject command = msg.body();			

		userManager.removeUserAsParticipant(command.getString(getDBField("usernameField")), command.getString("studyID"), command.getString("participantID"))
		.onSuccess(res -> {
			msg.reply(SoileCommUtils.successObject());
		})
		.onFailure(err -> handleError(err, msg));						    							

	}

	/**
	 * Get the participant for the provided user in the given project. 
	 * @param msg
	 */
	void setUserInfo(Message<JsonObject> msg)
	{
		JsonObject command = msg.body();			
		String username = (String) command.remove("username");
		userManager.setUserInfo(username, command)
		.onSuccess(res -> {
			msg.reply(SoileCommUtils.successObject().put(SoileCommUtils.DATAFIELD, res));
		})
		.onFailure(err -> handleError(err, msg));						    							
	}

	/**
	 * Set the password for the user given in the message to the password given in the message 
	 * @param msg
	 */
	void setPassword(Message<JsonObject> msg)
	{
		JsonObject command = msg.body();			
		String username = (String) command.remove("username");
		String password = (String) command.remove("password");
		userManager.setPassword(username, password)
		.onSuccess(res -> {
			msg.reply(SoileCommUtils.successObject());
		})
		.onFailure(err -> handleError(err, msg));						    							
	}

	/**
	 * Get the participant for the provided user in the given project. 
	 * @param msg
	 */
	void getUserInfo(Message<JsonObject> msg)
	{
		JsonObject command = msg.body();			
		String username = (String) command.remove("username");
		userManager.getUserInfo(username)
		.onSuccess(res -> {
			if(res == null)
			{
				handleError(new UserDoesNotExistException(username), msg);
				return;
			}
			// in case, we translate to the actual json Object
			JsonObject response = new JsonObject();
			response.put("username", res.getString(SoileConfigLoader.getUserdbField("usernameField")))
			.put("fullname", res.getString(SoileConfigLoader.getUserdbField("userFullNameField")))
			.put("role", res.getJsonArray(SoileConfigLoader.getUserdbField("userRolesField")).getString(0))
			.put("email", res.getString(SoileConfigLoader.getUserdbField("userEmailField")));
			msg.reply(SoileCommUtils.successObject().put(SoileCommUtils.DATAFIELD, response));
		})
		.onFailure(err -> handleError(err, msg));						    							
	}



	/**
	 * Handle an error using the error message from the throwable
	 * @param error the error that occured
	 * @param request the request which to respond to
	 */
	private void handleError(Throwable error, Message request)
	{
		
		handleError(error, request, null);
	}

	/**
	 * Fail a request with the given message.
	 * @param error the error that occured.
	 * @param request the request which failed.
	 * @param message the message to supply to in the result.
	 */
	private void handleError(Throwable error, Message request, String message)
	{
		// these are all expected errors, so no printing.
		if(message == null)
		{
			message = error.getMessage();
		}

		if(error instanceof UserDoesNotExistException)
		{
			request.fail(HttpURLConnection.HTTP_GONE, message);
			return;
		}
		if(error instanceof UserAlreadyExistingException)
		{
			request.fail(HttpURLConnection.HTTP_CONFLICT, "User already exists");
			return;
		}
		if(error instanceof EmailAlreadyInUseException)
		{
			request.fail(HttpURLConnection.HTTP_CONFLICT, "Email already in use");
			return;
		}
		if(error instanceof InvalidPermissionTypeException || error instanceof InvalidRoleException || error instanceof InvalidEmailAddress)
		{
			request.fail(HttpURLConnection.HTTP_BAD_REQUEST, message);
			return;
		}
		if(error instanceof HttpException)
		{			
			HttpException err = (HttpException)error;
			request.fail(err.getStatusCode(), err.getPayload());
			return;
		}
		
		LOGGER.error("Got an internal Server error");
		LOGGER.error(error);
		request.fail(500, message);
	}


	/**
	 * Get the type of permission change.
	 * @param change - The string indicating the type of change.
	 * @return the {@link PermissionChange} Object.
	 */
	public static PermissionChange getChange(String change) throws HttpException
	{
		switch(change)
		{
		case "set": 
			return PermissionChange.Set;
		case "add": 
			return PermissionChange.Add;
		case "remove":
			return PermissionChange.Remove;
		case "update":
			return PermissionChange.Update;
		default: 
			throw new HttpException(400, "Invalid Permission change type");
		}
	}

	/**
	 * Get the Auth Options according to the type requested (this mainly indicates the correct permission field! For role checks the type doesn't matter.
	 * @param type the Type to obtain the permissions for.
	 * @return
	 */
	private MongoAuthorizationOptions getOptionForType(String type) throws HttpException
	{
		try {
			TargetElementType elementType = TargetElementType.valueOf(type);
			switch(elementType)
			{
			case TASK: return SoileConfigLoader.getMongoTaskAuthorizationOptions();
			case EXPERIMENT: return SoileConfigLoader.getMongoExperimentAuthorizationOptions();
			case PROJECT: return SoileConfigLoader.getMongoProjectAuthorizationOptions();
			case STUDY: return SoileConfigLoader.getMongoStudyAuthorizationOptions();
			default: return null;
			}
		}
		catch(IllegalArgumentException e)
		{
			throw new HttpException(400, "Invalid type");
		}
	}

	/**
	 * Convert the given permissions into permisions stored in the permission array. 
	 * Permissions are provided as tuples with "type" and "target", where type is one of READ_WRITE, FULL, or READ, and target is the object id of the object for which the permissions are granted.
	 * The resulting permission strings are concatenations of "type + $ + targetid". 
	 * @param sourceArray The {@link JsonArray} with the permission tuples
	 * @return A list of concatenated strings.
	 * @throws InvalidPermissionTypeException if the type is not one of the types indicated above.
	 */
	private JsonArray convertPermissionsArray(JsonArray sourceArray) throws HttpException
	{
		
		JsonArray result = new JsonArray();
		for(int i = 0; i < sourceArray.size(); ++i)
		{
			JsonObject currentPermission = sourceArray.getJsonObject(i);
			String permissionType = currentPermission.getString("type");
			try
			{
				PermissionType.valueOf(permissionType);
			}
			catch(IllegalArgumentException | NullPointerException e)
			{
				throw new HttpException(400, permissionType + " is not a valid type for permissions");
			}
			result.addAll(SoilePermissionProvider.buildPermissionStringFromAPIPermission(currentPermission));
		}
		return result;
	}

	/**
	 * Convert the given permissions into permissions stored in the permission array. 
	 * Permissions are provided as tuples with "type" and "target", where type is one of READ_WRITE, FULL, or READ, and target is the object id of the object for which the permissions are granted.
	 * The resulting permission strings are concatenations of "type + $ + targetid". 
	 * @param sourceArray The {@link JsonArray} with the permission tuples
	 * @return A list of concatenated strings.
	 * @throws InvalidPermissionTypeException if the type is not one of the types indicated above.
	 */
	private JsonArray convertStringPermissions(JsonArray sourceArray)
	{		
		JsonArray result = new JsonArray();
		for(int i = 0; i < sourceArray.size(); ++i)
		{
			String currentPermission = sourceArray.getString(i);		
			result.add(SoilePermissionProvider.getAPIPermissionFromPermissionString(currentPermission));
		}
		return result;
	}
}
