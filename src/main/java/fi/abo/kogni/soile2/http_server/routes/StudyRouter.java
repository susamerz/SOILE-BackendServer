package fi.abo.kogni.soile2.http_server.routes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.aalto.scicomp.zipper.FileDescriptor;
import fi.aalto.scicomp.zipper.Zipper;
import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeFile;
import fi.abo.kogni.soile2.datamanagement.datalake.ParticipantDataLakeManager;
import fi.abo.kogni.soile2.http_server.auth.AccessHandler;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.PermissionType;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.TargetElementType;
import fi.abo.kogni.soile2.http_server.userManagement.exceptions.UserDoesNotExistException;
import fi.abo.kogni.soile2.http_server.verticles.DataBundleGeneratorVerticle.DownloadStatus;
import fi.abo.kogni.soile2.projecthandling.participant.ParticipantHandler;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.Study;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.impl.StudyHandler;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;

/**
 * Router for Study operations (i.e. project execution, and data retrieval).
 * Documentation for public functions (aside from the constructor) can be obtained from the 
 * API document, with operationIDs mapping 1:1 to method names 
 * @author Thomas Pfau
 *
 */
public class StudyRouter extends SoileRouter {

	StudyHandler studyHandler;
	AccessHandler studyAccessHandler;
	AccessHandler projectAccessHandler;
	ParticipantHandler partHandler;
	ParticipantDataLakeManager dataLakeManager;
	EventBus eb;
	Vertx vertx;

	static final Logger LOGGER = LogManager.getLogger(StudyRouter.class);	

	/**
	 * Default constructor
	 * @param auth The {@link SoileAuthorization} for auth checks
	 * @param vertx The {@link Vertx} instance for communication
	 * @param client the {@link MongoClient} for db access
	 * @param partHandler the {@link ParticipantHandler} for simplified participant checks
	 * @param projHandler the {@link StudyHandler} for simplified study checks
	 */
	public StudyRouter(SoileAuthorization auth, Vertx vertx, MongoClient client, ParticipantHandler partHandler, StudyHandler projHandler) {
		super(auth,client);
		eb = vertx.eventBus();
		this.vertx = vertx;		
		studyHandler = projHandler;
		this.partHandler = partHandler;						
		dataLakeManager = new ParticipantDataLakeManager(SoileConfigLoader.getServerProperty("soileResultDirectory"), vertx);
		studyAccessHandler = new AccessHandler(studyAuth, studyIDAccessHandler, roleHandler);
		projectAccessHandler = new AccessHandler(projectAuth, projectIDAccessHandler, roleHandler);
	}

	/**
	 * Create a Study from a Project 
	 * @param context The {@link RoutingContext} indicating the Project to build a study for
	 */
	public void startProject(RoutingContext context)
	{
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String id = params.pathParameter("id").getString();
		String version = params.pathParameter("version").getString();
		JsonObject sourceProjectInfo = new JsonObject().put("UUID", id).put("version", version);
		JsonObject projectData = new JsonObject().put("sourceProject", sourceProjectInfo).mergeIn(context.body().asJsonObject());
		// we need to check, whether the user has access to the actual project indicated.
		projectAuth.getAuthorizations(context.user())
		.onSuccess(Void -> {
			projectIDAccessHandler.authorize(context.user(), id, false, PermissionType.READ)			
			.onSuccess(canCreate -> {
				studyHandler.createStudy(projectData)
				.onSuccess(study -> {
					JsonObject permissionChange = new JsonObject().put("command", "add")
							.put("username", context.user().principal().getString("username"))
							.put("permissionsProperties", new JsonObject().put("elementType", TargetElementType.STUDY.toString())
									.put("permissionSettings", new JsonArray().add(new JsonObject().put("type", PermissionType.FULL.toString())
											.put("target", study.getID()))));
					eb.request("soile.umanager.permissionOrRoleChange", permissionChange)
					.onSuccess(success -> {
						// instance was created, access was updated, everything worked fine. Now
						context.response().setStatusCode(200)
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(new JsonObject().put("projectID",study.getID()).encode());												
					})
					.onFailure(err -> handleError(err, context));
				})
				.onFailure(err -> handleError(err, context));
			})
			.onFailure(err -> handleError(new HttpException(403, err.getMessage()), context));
		})
		.onFailure(err -> {
			handleError(err, context);
		});		
	}

	/**
	 * Get the list of available Studyies for the context (contaiing the user info)
	 * @param context The {@link RoutingContext} indicating the user.
	 */
	public void getStudyList(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		RequestParameter accessParam = params.queryParameter("access");
		Boolean full = params.queryParameter("full") == null ? false : params.queryParameter("full").getBoolean();
		if(full)
		{
			studyAccessHandler.checkAccess(context.user(),null, Roles.Admin,null,true)
			.onSuccess(allowed -> {
				studyHandler.getStudyList()		
				.onSuccess(elementList -> {	
					// this list needs to be filtered by access

					context.response()
					.setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(elementList.encode());
				})
				// this is a re
				.onFailure(err -> handleError(err, context));
			})
			.onFailure(err -> handleError(err, context));
		}
		else
		{
			studyAccessHandler.checkAccess(context.user(),null, Roles.Researcher,null,true)
			.onSuccess(allowed -> {
				String access = "general";				
				if(accessParam != null)
				{
					access = accessParam.getString();			
				}		
				Future<JsonArray> permissionsFuture;
				switch(access)		
				{
					case "read": permissionsFuture = authorizationRertiever.getReadPermissions(context.user(), TargetElementType.STUDY); break;
					case "write": permissionsFuture = authorizationRertiever.getWritePermissions(context.user(), TargetElementType.STUDY); break; 
					case "full": permissionsFuture = authorizationRertiever.getFullPermissions(context.user(), TargetElementType.STUDY); break;
					default: permissionsFuture = authorizationRertiever.getReadPermissions(context.user(),TargetElementType.STUDY); break;
				}							
				permissionsFuture
				.onSuccess( permissions -> {
					studyHandler.getStudyList(permissions, true)		
					.onSuccess(elementList -> {	
						// this list needs to be filtered by access

						context.response()
						.setStatusCode(200)
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(elementList.encode());
					})
					// this is a re					
					.onFailure(err -> handleError(err, context));	
				})
				// this is a re
				.onFailure(err -> handleError(err, context));	
			})
			.onFailure(err -> handleError(err, context));
		}
	}
	/**
	 * Get the running projects visible to the user in the context
	 * @param context The {@link RoutingContext} containing the user.
	 */
	public void getRunningProjectList(RoutingContext context)
	{						
		authorizationRertiever.getGeneralPermissions(context.user(),TargetElementType.STUDY)
		.onSuccess( permissions -> {
			LOGGER.debug("Permissions retrieved");
			studyHandler.getRunningStudyList(permissions, false)		
			.onSuccess(elementList -> {	
				// this list needs to be filtered by access
				context.response()
				.setStatusCode(200)
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
				.end(elementList.encode());
			})
			// this is a re
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> {
			if(err instanceof UserDoesNotExistException)
			{
				JsonArray permissions = new JsonArray();
				studyHandler.getRunningStudyList(permissions, false)
				.onSuccess(elementList -> {	
					// this list needs to be filtered by access

					context.response()
					.setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(elementList.encode());
				})
				.onFailure(newerr -> handleError(newerr, context));
			}
			else
			{
				handleError(err, context);
			}
		});	
	}


	/**
	 * Stop a Study (deactivate it)
	 * @param context The Context indicating the Study (and user)
	 */
	public void stopProject(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();

		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.FULL,true)
		.onSuccess(Void -> 
		{
			studyHandler.deactivate(requestedInstanceID)
			.onSuccess(project -> {	
					context.response()
					.setStatusCode(200)						
					.end();
			
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));			
	}

	/**
	 * Start a study (make it active and useable)
	 * @param context The Context indicating the {@link Study}
	 */
	public void startStudy(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();

		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.FULL,true)
		.onSuccess(Void -> 
		{
			studyHandler.activate(requestedInstanceID)
			.onSuccess(project -> {	
				// this list needs to be filtered by access
					context.response()
					.setStatusCode(200)						
					.end();
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));			
	}
	/**
	 * Delete a Study entirely.
	 * TODO: Rename this (problem this is part of the API description)
	 * @param context The {@link RoutingContext} indicating the Study
	 */
	public void deleteProject(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.FULL,true)
		.onSuccess(Void -> 
		{
			studyHandler.deleteStudy(requestedInstanceID)
			.onSuccess(deletedObject -> {
				@SuppressWarnings("rawtypes")
				List<Future<Void>> deletionFutures = new LinkedList<Future<Void>>();
				for(int i = 0; i < deletedObject.getJsonArray("participants").size(); ++i)
				{
					deletionFutures.add(partHandler.deleteParticipant(deletedObject.getJsonArray("participants").getString(i), false));
				}
				Future.all(deletionFutures)
				.onSuccess(done -> {					
					context.response()
					.setStatusCode(200)						
					.end();

				})
				.onFailure(err -> { 
					// the study was deleted from the handler and the db, but something went wrong when deleting the 
					LOGGER.error("Something went wrong when deleting the study. the original db entry was: " + deletedObject.encodePrettily());
					handleError(err, context);
					});
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));			
	}

	/**
	 * Reset the Study given in the context (clean up all currently saved data)
	 * @param context The {@link RoutingContext} indicating the study
	 */
	public void resetStudy(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.FULL,true)
		.onSuccess(Void -> 
		{
			studyHandler.loadPotentiallyOutdatedStudy(requestedInstanceID)
			.onSuccess(study -> {					
				//
				
				study.reset()
				.onSuccess(participantsToDelete -> {
					@SuppressWarnings("rawtypes")
					List<Future<Void>> deletionFutures = new LinkedList<Future<Void>>();
					for(int i = 0; i < participantsToDelete.size(); ++i)
					{
						deletionFutures.add(partHandler.deleteParticipant(participantsToDelete.getString(i), false));
					}
					Future.all(deletionFutures)
					.onSuccess(done -> {
							context.response()
							.setStatusCode(200)						
							.end();
					})
					.onFailure(err -> handleError(err, context));
					
				})
				.onFailure(err -> handleError(err, context));
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));			
	}
	/**
	 * List downloadable data for the Study in the context
	 * @param context The {@link RoutingContext} indicating the Study
	 */
	public void listDownloadData(RoutingContext context)
	{
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();

		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void -> {
			studyHandler.loadUpToDateStudy(requestedInstanceID)
			.onSuccess(project -> {					
				//JsonArray taskData = project.getTasksWithNames();
				// this list needs to be filtered by access
				partHandler.getParticipantStatusForProject(project)				
				.onSuccess(participantStatus -> {									
					JsonObject response = new JsonObject();
					response.put("participants", participantStatus)
					.put("tasks", project.getTasksInstancesWithNames());
					context.response()
					.setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(response.encode());					
				})
				.onFailure(err -> handleError(err, context));
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));		
	}
	/**
	 * Update the data of the Study (only possible when study is not active.
	 * @param context The {@link RoutingContext} indicating the Study (and the update)
	 */
	public void updateStudy(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		//TODO: not implemented properly yet
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ_WRITE,false)
		.onSuccess(Void -> 
		{			
			LOGGER.debug("Access granted, updating Study");
			studyHandler.updateStudy(requestedInstanceID,context.body().asJsonObject())
			.onSuccess(studyUpdated -> {
				context.response()
				.setStatusCode(200)				
				.end();
			})
			.onFailure(err -> handleError(err, context));										
		})
		.onFailure(err -> handleError(err, context));			
	}
	/**
	 * Get the properties of the Study indicated in the given {@link RoutingContext}
	 * @param context The {@link RoutingContext} containing the study id.
	 */
	public void getStudyProperties(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		//TODO: not implemented properly yet
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void -> 
		{			
			studyHandler.loadUpToDateStudy(requestedInstanceID)			
			.onSuccess(study -> {
				study.isActive()
				.onSuccess(active -> {
					// we add the "Active" field, since it is a useful property.
					context.response()
					.setStatusCode(200)				
					.end(study.toAPIJson().put("active", active).encode());
				})
				.onFailure(err -> handleError(err, context));	
			})
			.onFailure(err -> handleError(err, context));										
		})
		.onFailure(noAccess -> {
			studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Participant,PermissionType.EXECUTE,false)
			.onSuccess( participantAccess -> {
				studyHandler.loadUpToDateStudy(requestedInstanceID)			
				.onSuccess(study -> {
					study.isActive()
					.onSuccess(active -> {
						// we add the "Active" field, since it is a useful property.
						JsonObject studyInfo = study.toAPIJson();
						JsonObject responseObject = new JsonObject().put("UUID", study.getID())
																	.put("name", study.getName())
																	.put("shortDescription", studyInfo.getValue("shortDescription"))
																	.put("description", studyInfo.getValue("description"))
																	.put("shortcut", study.getShortCut());
						context.response()
						.setStatusCode(200)				
						.end(responseObject.encode());
					})
					.onFailure(err -> handleError(err, context));	
				})
				.onFailure(err -> handleError(err, context));
				
			})
			.onFailure(err -> handleError(err, context));			
		});			
	}
	
	/**
	 * Get the Study Results
	 * @param context The {@link RoutingContext} indicating the Study to get the results for.
	 */
	public void getProjectResults(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		//TODO: not implemented properly yet
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void -> 
		{			
			// this list needs to be filtered by access
			JsonObject requestBody = null;
			try
			{
				requestBody = context.body().asJsonObject();
				if(requestBody.fieldNames().size() != 1)
				{
					handleError(new HttpException(400, "Invalid request"), context);
					return;
				}
				else
				{
					// just add whichever name is there as the request type.
					requestBody.put("requestType", requestBody.fieldNames().iterator().next());					
				}
			}
			catch(Exception e)
			{
				// this is a request All request.
				requestBody = new JsonObject().put("requestType", "all"); 
			}
			requestBody.put("projectID", requestedInstanceID);

			eb.request("fi.abo.soile.DLCreate", requestBody)
			.onSuccess(response -> {
				String dlID = response.body().toString();
				context.response()
				.setStatusCode(200)
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
				.end(new JsonObject().put("downloadID",dlID).encode());
			})
			.onFailure(err -> handleError(err, context));										
		})
		.onFailure(err -> handleError(err, context));			
	}
	/**
	 * Check whether a specified download is read
	 * @param context The {@link RoutingContext} indicating the download to check
	 */
	public void downloadTest(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		String dlID = params.pathParameter("downloadid").getString();
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void -> 
		{
			// this list needs to be filtered by access
			eb.request("fi.abo.soile.DLStatus", new JsonObject().put("downloadID",dlID))
			.onSuccess(response -> {
				JsonObject responseBody = (JsonObject) response.body();					
				context.response()
				.setStatusCode(200)
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
				.end(responseBody.encode());
			})
			.onFailure(err -> handleError(err, context));										
		})
		.onFailure(err -> handleError(err, context));			
	}	

	/**
	 * Download the Results specfied in the {@link RoutingContext}
	 * @param context The {@link RoutingContext} specifying which download to retrieve
	 */
	public void downloadResults(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		String dlID = params.pathParameter("downloadid").getString();

		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void -> 
		{
			// this list needs to be filtered by access
			LOGGER.debug("Requesting finished download file from Eventbus for id " + dlID);
			eb.request("fi.abo.soile.DLFiles", new JsonObject().put("downloadID",dlID))
			.onSuccess(response -> {				
				JsonObject responseBody = (JsonObject) response.body();
				if(responseBody.getString("status").equals(DownloadStatus.downloadReady.toString()))
				{
					List<FileDescriptor> dLFiles = new LinkedList<>();
					for(int i = 0; i < responseBody.getJsonArray("files").size(); i++)
					{
						DataLakeFile temp = new DataLakeFile(responseBody.getJsonArray("files").getJsonObject(i));
						dLFiles.add(temp);
					}
					try
					{
						Zipper pump = new Zipper(vertx, dLFiles.iterator());
						// the response is a chunked zip file.
						context.response().putHeader("content-type", "application/zip")
						.putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + dlID + ".zip\"")
						.setChunked(true);						
						pump.pipeTo(context.response()).onSuccess(success -> {
							LOGGER.debug("Download " + dlID + " successfullytransmitted");							
						}).onFailure(err -> {
							LOGGER.error("Download " + dlID + " failed");
							LOGGER.error(err);
							context.response().end();
						});											
					}
					catch(IOException e)
					{
						handleError(e, context);
					}			
				}
				else
				{
					LOGGER.debug("Status was: " + responseBody.getString("status") + " // While ready status should be: " + DownloadStatus.downloadReady.toString());

					context.response()
					.setStatusCode(503)					
					.end("Download not yet ready");

				}
			})
			.onFailure(err -> handleError(err, context));										
		})
		.onFailure(err -> handleError(err, context));			
	}
	
	/**
	 * Get Information about the tokens set in a study
	 * @param context The {@link RoutingContext} indicating the {@link Study} for which to obtain data
	 */
	public void getTokenInformation(RoutingContext context)
	{
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();
		
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void -> 
		{
			studyHandler.loadPotentiallyOutdatedStudy(requestedInstanceID)
			.onSuccess(currentStudy -> {
				currentStudy.getTokenInformation()
				.onSuccess(tokenInformation -> {
					context.response()
					.setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(tokenInformation.encode());
				})
				.onFailure(err -> handleError(err, context));
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));		
	}

	/**
	 * Create Tokens for a {@link Study}
	 * @param context The {@link RoutingContext} indicating which Study to generate tokens for
	 */
	public void createTokens(RoutingContext context)
	{				
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String requestedInstanceID = params.pathParameter("id").getString();

		boolean unique = params.queryParameter("unique") == null ? false : params.queryParameter("unique").getBoolean();
		int count = params.queryParameter("count") == null ? 0 : params.queryParameter("count").getInteger();

		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.FULL,false)
		.onSuccess(Void -> 
		{
			studyHandler.loadPotentiallyOutdatedStudy(requestedInstanceID)
			.onSuccess(instance -> {
				if(unique)
				{
					instance.createPermanentAccessToken()
					.onSuccess(token -> {
						context.response()
						.setStatusCode(200)
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(token);
					})
					.onFailure(err -> handleError(err, context));
				}
				else
				{

					instance.createSignupTokens(count).
					onSuccess(tokenArray -> {
						LOGGER.debug("Replying with: \n " + tokenArray.encodePrettily());
						context.response()
						.setStatusCode(200)
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(tokenArray.encode());
					})
					.onFailure(err -> handleError(err, context));
				}

			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));			
	}	
	
	
	/**
	 * Get the collaborators for the given Study using the Context to extract the study from.
	 * @param context the {@link RoutingContext} to extract information from 
	 */
	public void getCollaboratorsForStudy(RoutingContext context)
	{
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);						
		String requestedInstanceID = params.pathParameter("id").getString();
		
		studyAccessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void ->	{	
			vertx.eventBus().request("soile.umanager.getCollaboratorsforStudy", new JsonObject().put("studyID",requestedInstanceID))
			.onSuccess(res -> {
				JsonObject result = (JsonObject)res.body();
				if(result.getString(SoileCommUtils.RESULTFIELD).equals(SoileCommUtils.SUCCESS))
				{
					context.response()
					.setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(result.getJsonArray(SoileCommUtils.DATAFIELD).encode());
				}
				else
				{
					handleError(new Exception(result.getString(SoileCommUtils.REASONFIELD)), context);
				}
			})
			.onFailure(err -> handleError(err,context));
		})
		.onFailure(err -> handleError(err,context));	
	}
	
}
