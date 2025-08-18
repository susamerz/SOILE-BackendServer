package fi.abo.kogni.soile2.http_server.routes;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.aalto.scicomp.zipper.Zipper;
import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeResourceManager;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.PermissionType;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.http_server.requestHandling.IDSpecificFileProvider;
import fi.abo.kogni.soile2.http_server.requestHandling.NonStaticHandler;
import fi.abo.kogni.soile2.projecthandling.apielements.APITask;
import fi.abo.kogni.soile2.projecthandling.projectElements.TaskBundler;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.FileSystemAccess;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;

/**
 * The Task Router (of the element Router needs a couple of additional routes like Resource Posting/retrieval). 
 * @author Thomas Pfau
 *
 */
public class TaskRouter extends ElementRouter<Task> {

	private static final Logger LOGGER = LogManager.getLogger(TaskRouter.class);
	NonStaticHandler libraryHandler;
	IDSpecificFileProvider resourceHandler;
	TaskBundler bundler;
	Vertx vertx;
	/**
	 * Default constructor
	 * All Functions receiving a routing context use that routing context to provide the Response.
	 * @param client the {@link MongoClient} for DB access
	 * @param resManager the {@link IDSpecificFileProvider} used for file retrieval.
	 * @param vertx The {@link Vertx} instance for communication
	 * @param auth The {@link SoileAuthorization} for authorization checks.
	 */
	public TaskRouter(MongoClient client, IDSpecificFileProvider resManager, Vertx vertx, SoileAuthorization auth)
	{
		super(ElementManager.getTaskManager(client,vertx),auth, vertx.eventBus(), client);
		libraryHandler = new NonStaticHandler(FileSystemAccess.ROOT, SoileConfigLoader.getServerProperty("taskLibraryFolder"), "/lib/");
		resourceHandler = resManager;
		bundler = new TaskBundler(new DataLakeResourceManager(vertx), eb, vertx, elementManager);
		this.vertx = vertx;
	}		

	/**
	 * Add a resource to a task
	 * @param context the {@link RoutingContext} containing the resource and the task
	 */
	public void putResource(RoutingContext context)
	{				
		LOGGER.debug(context.pathParam("id") + "/" + context.pathParam("version") + "/" + context.pathParam("*") );				
		String elementID = context.pathParam("id");
		String version = context.pathParam("version");
		String filename = normalizePath(context.pathParam("*"));
		Boolean delete = context.queryParams().get("delete") != null ? Boolean.parseBoolean(context.queryParams().get("delete")) : false;	
		if(filename.startsWith("lib/"))
		{
			handleError(new HttpException(400, "lib/ is a restricted path!"), context);
			return;
		}
		
		accessHandler.checkAccess(context.user(),elementID, Roles.Researcher,PermissionType.READ_WRITE,true)
		.compose(allowed -> { return checkVersionAndID(elementID, version); })
		.onSuccess(Void -> 
		{
			if(delete)
			{
				elementManager.handleDeleteFile(elementID, version, filename)
				.onSuccess(newversion -> {
					context.response().setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
					.end(new JsonObject().put("version", newversion).encode());
				})
				.onFailure(err -> handleError(err, context));
			}
			else
			{
				if(context.fileUploads().size() == 0)
				{
					handleError(new HttpException(400, "Missing or invalid file data, exactly one File expected"), context);
					return;
				}
				if(!(filename.endsWith("/") || filename.equals("")) && context.fileUploads().size() > 1)
				{
					handleError(new HttpException(400, "Cannot upload multiple files to one target file"), context);
					return;
				}				
				elementManager.handlePostFiles(elementID, version, filename, context.fileUploads())
				.onSuccess(newversion -> {
					context.response().setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
					.end(new JsonObject().put("version", newversion).encode());
				})
				.onFailure(err -> handleError(err, context));
								

			}
		})
		.onFailure(err -> handleError(err, context));
	}
	/**
	 * Get a resource for a task
	 * @param context The {@link RoutingContext} specifying the Task and resource to get
	 */
	public void getResource(RoutingContext context)
	{
		String elementID = context.pathParam("id");
		String version = context.pathParam("version");
		String filename = normalizePath(context.pathParam("*")); 	
		accessHandler.checkAccess(context.user(),elementID, Roles.Researcher,PermissionType.READ,true)
		.compose(allowed -> { return checkVersionAndID(elementID, version); })
		.onSuccess(Void -> 
		{
			// Potentially update with fileProvider.
			elementManager.handleGetFile(elementID, version, filename )
			.onSuccess( file -> {
				String contentType = file.getFormat();
				if (contentType.startsWith("text")) {
					context.response().putHeader(HttpHeaders.CONTENT_TYPE, contentType + ";charset=" + Charset.defaultCharset().name());
				} else {
					context.response().putHeader(HttpHeaders.CONTENT_TYPE, contentType);
				}
				context.response().sendFile(file.getPath()).andThen(res2 -> {
					if (res2.failed()) {
						if (!context.request().isEnded()) {
							context.request().resume();
						}
						context.fail(res2.cause());
					}
				});				
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));
	}
	
	/**
	 * Get the code options available  
	 * @param context The {@link RoutingContext} for auth checks
	 */
	public void getCodeOptions(RoutingContext context)
	{
		accessHandler.checkAccess(context.user(),null, Roles.Researcher,null,true)
		.onSuccess(Void -> 
		{
			context.response()
			.setStatusCode(200)
			.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
			.end(SoileConfigLoader.getAvailableTaskOptions().encode());			
		})
		.onFailure(err -> handleError(err, context));
	}
	
	
	/**
	 * Get the compiled code for the given task
	 * @param context The {@link RoutingContext} indicating the {@link Task}
	 */
	public void getCompiledTask(RoutingContext context)
	{
		String elementID = context.pathParam("id");
		String version = context.pathParam("version");		 
		accessHandler.checkAccess(context.user(),elementID, Roles.Researcher,PermissionType.READ,true)
		.compose(allowed -> { return checkVersionAndID(elementID, version); })
		.onSuccess(Void -> 
		{
			elementManager.getAPIElementFromDB(elementID, version).onSuccess(
			element -> {		
				APITask currentTask = (APITask) element;				
				eb.request(SoileConfigLoader.getVerticleProperty("gitCompilationAddress"),
						new JsonObject().put("UUID", element.getUUID())
						.put("type", currentTask.getCodeType())
						.put("version", element.getVersion()))
				.onSuccess(response -> {
					JsonObject responseBody = (JsonObject) response.body();
					context.response()
					.setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, SoileConfigLoader.getMimeTypeForTaskLanugage(currentTask.getCodeLanguage()))
					.end(responseBody.getString("code"));
				})
			.onFailure(err -> handleError(err, context));
			})
			.onFailure(err -> handleError(err, context));

		})
		.onFailure(err -> handleError(err, context));
	}
	
	/**
	 * Compile code for the given Task
	 * @param context The {@link RoutingContext} containing the Task to compile the code for
	 */
	public void compileCode(RoutingContext context)
	{
		
		accessHandler.checkAccess(context.user(),null, Roles.Researcher,null,true)
		.onSuccess(Void -> 
		{
			
			JsonObject codeInfo = context.body().asJsonObject();
			eb.request(SoileConfigLoader.getVerticleProperty("compilationAddress"),codeInfo)
				.onSuccess(response -> {
					LOGGER.debug(response.body());
					JsonObject responseBody = (JsonObject) response.body();
					context.response()
					.setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, SoileConfigLoader.getMimeTypeForTaskLanugage(codeInfo.getString("type")))
					.end(responseBody.getString("code"));
				})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));
	}
	
	/**
	 * Get a library for the given Task
	 * @param context The {@link RoutingContext} indicating the library to obtain
	 */
	public void getLib(RoutingContext context)
	{
		String requestedInstanceID = context.pathParam("id");

		accessHandler.checkAccess(context.user(),requestedInstanceID, Roles.Researcher,PermissionType.READ,false)
		.onSuccess(Void -> {
				//JsonArray taskData = project.getTasksWithNames();
				// this list needs to be filtered by access				
				libraryHandler.handle(context);							
		})
		.onFailure(err -> handleError(err, context));		
	}

	/**
	 * Get the files associated with the Task
	 * @param context The {@link RoutingContext} indicating the Task 
	 */
	public void getTaskFileList(RoutingContext context)
	{
		String elementID = context.pathParam("id");
		String version = context.pathParam("version");		 

		accessHandler.checkAccess(context.user(),elementID, Roles.Researcher,PermissionType.READ,false)
		.compose(allowed -> { return checkVersionAndID(elementID, version); })
		.onSuccess(Void -> {
			eb.request("soile.git.getResourceList", new JsonObject().put("repoID", elementManager.getGitIDForUUID(elementID)).put("version", version))
			.onSuccess(response -> {				
				JsonArray responseBody = (JsonArray)response.body();
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
	 * Get Resources for execution of a task
	 * @param context The {@link RoutingContext} indicating the requested resource
	 */
	public void getResourceForExecution(RoutingContext context)
	{
		String elementID = context.pathParam("id");
		accessHandler.checkAccess(context.user(),elementID, Roles.Participant,PermissionType.EXECUTE,false)
		.onSuccess(Void -> {			
			resourceHandler.handleContext(context, elementManager.getElementSupplier().get());			
		})
		.onFailure(err -> handleError(err, context));		
	}

	/**
	 * Download the specified task
	 * @param context The {@link RoutingContext} speifiying the {@link Task}  to download
	 */
	public void downloadTask(RoutingContext context)
	{		
		RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		String elementID = params.pathParameter("id").getString();
		String version = params.pathParameter("version").getString();
		accessHandler.checkAccess(context.user(),elementID, Roles.Researcher,PermissionType.READ,true)
		.compose(allowed -> { return checkVersionAndID(elementID, version); })
		.onSuccess(Void -> {
			bundler.buildTaskFileList(elementID, version)
			.onSuccess(fileList -> {
				try {
					Zipper pump = new Zipper(vertx, fileList.iterator());
					// the response is a chunked zip file.
					context.response().putHeader("content-type", "application/zip")
					.putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + elementID + ".zip\"")
					.setChunked(true);						
					pump.pipeTo(context.response()).onSuccess(success -> {
					}).onFailure(err -> {
						context.response().end();
					});											
				}
				catch(IOException e)
				{
					handleError(e, context);
				}			
			})
			.onFailure(err -> handleError(err, context));
		})
		.onFailure(err -> handleError(err, context));
	}
	
	/**
	 * Upload a Task from a Task Zip file
	 * @param context The {@link RoutingContext} containing the Task Zip file.
	 */
	public void uploadTask(RoutingContext context)
	{		
		LOGGER.debug("Received a request for creation");
		accessHandler.checkAccess(context.user(),null, Roles.Researcher,null,true)
		.onSuccess(Void -> 
		{
			LOGGER.debug("Request Access granted");
			List<String> nameParam = context.queryParam("name");
			List<String> tagParam = context.queryParam("tag");
			String newTaskName = null;
			if(nameParam.size() > 1) {
				LOGGER.debug("Invalid name parameter");
				handleError(new HttpException(400, "Must have provide a name"), context);
				return;
			}			
			else
			{
				if(nameParam.size() == 1)
				{
					newTaskName = nameParam.get(0);
				}
			}
			if(tagParam.size() != 1) {
				LOGGER.debug("Invalid tag parameter");
				handleError(new HttpException(400, "Must provide exactly one tag to use for the version created."), context);
				return;
			}	
			if(context.fileUploads().size() != 1)
			{
				handleError(new HttpException(400, "Missing or invalid file data, exactly one File expected"), context);
				return;
			}
			
			LOGGER.debug("Trying to upload Task");
			bundler.createTaskFromFile(new File(context.fileUploads().get(0).uploadedFileName()), tagParam.get(0), newTaskName)
			.onSuccess(element -> {	
				LOGGER.debug("Element Created");
				JsonObject permissionChangeRequest = new JsonObject()
						.put("username", context.user().principal().getString("username"))
						.put("command", "add")
						.put("permissionsProperties", new JsonObject().put("elementType", element.getElementType().toString())
																	  .put("permissionSettings",new JsonArray().add(new JsonObject().put("target", element.getUUID())
																			  														.put("type", PermissionType.FULL.toString()))
																		  )
							);	
				LOGGER.debug("Requesting permission change");
				eb.request("soile.umanager.permissionOrRoleChange", permissionChangeRequest)
				.onSuccess( reply -> {
					LOGGER.debug("Permissions added to user for "+ nameParam + "/" + element.getUUID());
					elementManager.getAPIElementFromDB(element.getUUID(), element.getCurrentVersion())
					.onSuccess(apiElement -> {
						LOGGER.debug("Api element created");
						context.response()
						.setStatusCode(200)
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(apiElement.getAPIJson().encode());			
					}).onFailure(err -> handleError(err, context));	
				})
				.onFailure(err -> handleError(err, context));						
			}).onFailure(err -> handleError(err, context));		
		})
		.onFailure(err -> handleError(err, context));
	}
	/**
	 * Clean up this Router (periodic cleanup).
	 */
	public void cleanup()
	{
		elementManager.cleanUp();
	}
}
