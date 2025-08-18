package fi.abo.kogni.soile2.http_server.routes;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.TargetElementType;
import fi.abo.kogni.soile2.http_server.auth.SoileIDBasedAuthorizationHandler;
import fi.abo.kogni.soile2.http_server.auth.SoileRoleBasedAuthorizationHandler;
import fi.abo.kogni.soile2.projecthandling.exceptions.ElementNameExistException;
import fi.abo.kogni.soile2.projecthandling.exceptions.ObjectDoesNotExist;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Experiment;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Project;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.AccessStudy;
import io.netty.handler.codec.http.HttpObjectDecoder;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.impl.HttpUtils;
import io.vertx.core.internal.net.RFC3986;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.mongo.MongoAuthorization;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;

/**
 * Base class which provides Error Handling for Soile Routers handling some common errors.
 * @author Thomas Pfau
 *
 */
public class SoileRouter {

	private static final Logger LOGGER = LogManager.getLogger(ElementRouter.class);

	/**
	 * The project Authorization
	 */
	protected MongoAuthorization projectAuth;
	/**
	 * The experiment Authorization
	 */
	protected MongoAuthorization experimentAuth;
	/**
	 * The task Authorization
	 */
	protected MongoAuthorization taskAuth;
	/**
	 * The study Authorization
	 */
	protected MongoAuthorization studyAuth;
	/**
	 * ID Access Handler for Tasks
	 */
	protected SoileIDBasedAuthorizationHandler taskIDAccessHandler;
	/**
	 * ID Access Handler for Projects
	 */
	protected SoileIDBasedAuthorizationHandler projectIDAccessHandler;
	/**
	 * ID Access Handler for experiments
	 */
	protected SoileIDBasedAuthorizationHandler experimentIDAccessHandler;
	/**
	 * ID Access Handler for Studies
	 */
	protected SoileIDBasedAuthorizationHandler studyIDAccessHandler;
	/**
	 * Authorization retriever
	 */
	protected SoileAuthorization authorizationRertiever;
	/**
	 * Role based authorization retriever
	 */
	protected SoileRoleBasedAuthorizationHandler roleHandler;
	
	
	/**
	 * Default constructor
	 * @param auth the {@link SoileAuthorization} for Auth checks
	 * @param client The {@link MongoClient} for DB access
	 */
	public SoileRouter(SoileAuthorization auth, MongoClient client)
	{
		authorizationRertiever = auth;
		projectAuth = auth.getAuthorizationForOption(TargetElementType.PROJECT);
		experimentAuth = auth.getAuthorizationForOption(TargetElementType.EXPERIMENT);
		taskAuth = auth.getAuthorizationForOption(TargetElementType.TASK);
		studyAuth = auth.getAuthorizationForOption(TargetElementType.STUDY);		
		taskIDAccessHandler = new SoileIDBasedAuthorizationHandler(new Task().getTargetCollection(), client);
		experimentIDAccessHandler = new SoileIDBasedAuthorizationHandler(new Experiment().getTargetCollection(), client);
		projectIDAccessHandler = new SoileIDBasedAuthorizationHandler(new Project().getTargetCollection(), client);
		studyIDAccessHandler = new SoileIDBasedAuthorizationHandler(new AccessStudy().getTargetCollection(), client, true);

		roleHandler = new SoileRoleBasedAuthorizationHandler();
	}
	
	/**
	 * Default handling of errors. 
	 * @param err the error to handle
	 * @param context the RoutingContext the Error occurs in
	 */
	public static void handleError(Throwable err, RoutingContext context)
	{
		LOGGER.error("Reporting error" , err);
		if(err instanceof ElementNameExistException)
		{	
			sendError(context, 409, err.getMessage());			
			return;
		}
		if(err instanceof ObjectDoesNotExist)
		{
			sendError(context, 404, err.getMessage());			
			return;
		}
		if(err instanceof HttpException)
		{
			HttpException e = (HttpException) err;
			sendError(context, e.getStatusCode(), e.getPayload() != null ? e.getPayload() : e.getMessage());						
			return;
		}
		if(err.getCause() instanceof ReplyException)
		{
			ReplyException rerr = (ReplyException)err.getCause();
			sendError(context, rerr.failureCode(), rerr.getMessage());			
			return;
		}
		if(err instanceof ReplyException)
		{	
			ReplyException rerr = (ReplyException)err;
			sendError(context, rerr.failureCode(), rerr.getMessage());			
			return;
		}
		sendError(context, 400, err.getMessage());
	}
	
	/**
	 * Send an error for the given context
	 * @param context the RoutingContext to send the error
	 * @param code the error cod to send
	 * @param message the message to send along the error
	 */
	static void sendError(RoutingContext context, int code, String message)
	{		
		LOGGER.error("Request errored. Returning code" + code + " with message " + message);
		context.response()
		.setStatusCode(code)
		.end(message);
	}
	
	/**
	 * Get the handler for a specific TargetElement 
	 * @param type the type of element to get the handler for
	 * @return The {@link SoileIDBasedAuthorizationHandler} corresponding to the right Target type
	 */
	protected SoileIDBasedAuthorizationHandler getHandlerForType(TargetElementType type)
	{
		switch(type)
		{
			case PROJECT: return projectIDAccessHandler;
			case EXPERIMENT: return experimentIDAccessHandler;
			case TASK: return taskIDAccessHandler;
			case STUDY: return studyIDAccessHandler;
			default: return taskIDAccessHandler;
		}
		
	}
	/**
	 * Get the handler for a specific TargetElement 
	 * @param type the type of element to get the handler for
	 * @return The {@link SoileIDBasedAuthorizationHandler} corresponding to the right Target type
	 */
	protected SoileIDBasedAuthorizationHandler getHandlerForType(String type)
	{
		return getHandlerForType(TargetElementType.valueOf(type));		
	}
	/**
	 * Get the right Authorization for the target type   
	 * @param type the type of element to get the handler for
	 * @return The {@link MongoAuthorization} corresponding to the right Target type
	 */
	protected MongoAuthorization getAuthForType(TargetElementType type)
	{		
		switch(type)
		{
			case PROJECT: return projectAuth;
			case EXPERIMENT: return experimentAuth;
			case TASK: return taskAuth;
			case STUDY: return studyAuth;
			default: return taskAuth;
		}
	}
	/**
	 * Get the right Authorization for the target type   
	 * @param type the type of element to get the handler for
	 * @return The {@link MongoAuthorization} corresponding to the right Target type
	 */
	protected MongoAuthorization getAuthForType(String type)
	{		
		return getAuthForType(TargetElementType.valueOf(type));
	}
	
	/**
	 * This is a function to test, whether the given user is a token user.
	 * @param user the user to check
	 * @return whether the user is a user based on a token, or not.
	 */
	public static boolean isTokenUser(User user)
	{
		return user.principal().containsKey("access_token") && !user.principal().containsKey("username");
	}

	/**
	 *  Function to normalize a path and return it in a normalized way.
	 *  @param inputPath The path to be normalized
	 *  @return the normalized path; 
	 */
	public static String normalizePath(String inputPath)
	{
		String uriDecodedPath = URLDecoder.decode(inputPath, StandardCharsets.UTF_8);
		// if the normalized path is null it cannot be resolved
		if (uriDecodedPath == null) {			
			return null;
		}
		// will normalize and handle all paths as UNIX paths
		String treatedPath = RFC3986.removeDotSegments(uriDecodedPath.replace('\\', '/'));
		return treatedPath;
	}
}
