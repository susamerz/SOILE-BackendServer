package fi.abo.kogni.soile2.http_server.auth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.PermissionType;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.projecthandling.exceptions.ObjectDoesNotExist;
import fi.abo.kogni.soile2.projecthandling.participant.ParticipantHandler;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.OrAuthorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
/**
 * Class that provides Authentication for token carrying requests. 
 * @author Thomas Pfau
 *
 */
public class TokenAuthProvider {
	static final Logger LOGGER = LogManager.getLogger(TokenAuthProvider.class);

	ParticipantHandler partHandler;
	MongoClient client;
	/**
	 * Default constructor
	 * @param partHandler the Partiicpant handler (to indicate the user)
	 * @param client the MongoClient for db communication
	 */
	public TokenAuthProvider(ParticipantHandler partHandler, MongoClient client)
	{
		this.partHandler = partHandler;
		this.client = client;
	}
	
	/**
	 * Authenticate the given token. The token must fit the studyID requested in the RoutingContext.
	 * If both match (i.e. the token is associated with a valid participant for the study, then the 
	 * Provider creates a user which does NOT have a username but only a token.
	 * TODO: Need to ensure that this is compatible with all relevant endpoints (i.e. the endpoints for project execution)  
	 * @param context the {@link RoutingContext} to authenticate
	 * @return A Future of the user to be added to the context.
	 */
	public Future<User> authenticate(RoutingContext context )
	{			
		String pathID = context.pathParam("id");
		String token = context.request().getHeader("Authorization");
		LOGGER.debug("Token Auth Provider for: " + token);
		if(token == null)
		{
			LOGGER.debug("No Token Auth!");
			return Future.failedFuture(new HttpException(401,"No Token"));
		}
		String projectID = token.substring(token.indexOf("$")+1);
		if(pathID == null)
		{
			pathID = projectID;
		}
		LOGGER.debug("Trying to retrieve TokenParticipant for Token: " + token);
		Promise<User> userPromise = Promise.promise();
		getID(pathID)
		.onSuccess(requestedStudyID -> {
			LOGGER.debug("Trying to retrieve participant for project: " + requestedStudyID);
			partHandler.getParticipantForToken(token, requestedStudyID)
			.onSuccess(participant -> {
				LOGGER.debug("Got participant");
				User currentUser = User.fromToken(token);
				// Token access is always participant access
				currentUser.principal().put(SoileConfigLoader.getSessionProperty("userRoles"), new JsonArray().add(Roles.Participant));
				currentUser.principal().put("tokenPermission", SoilePermissionProvider.buildPermissionString(requestedStudyID, PermissionType.EXECUTE));
				// we add 
				Authorization TokenAuth = OrAuthorization.create().addAuthorization(RoleBasedAuthorization.create(Roles.Participant.toString()))
										  .addAuthorization(SoilePermissionProvider.buildPermission(requestedStudyID, PermissionType.EXECUTE));				
				currentUser.authorizations().put("TokenProvider", TokenAuth);				
				userPromise.complete(currentUser);
			})
			.onFailure(err -> userPromise.fail(err));
		})
		.onFailure(err -> userPromise.fail(err));
		
		return userPromise.future();
		
	}
	
	private Future<String> getID(String pathID)
	{
		Promise<String> idPromise = Promise.promise();		 				
		JsonObject Query = new JsonObject().put("$or", new JsonArray().add(new JsonObject().put("shortcut",pathID))
																	  .add(new JsonObject().put("_id", pathID))
																	  );		
		client.findOne(SoileConfigLoader.getCollectionName("studyCollection"),Query,new JsonObject().put("_id",1))
		.onSuccess(project -> 
		{
			if(project == null)
			{
				idPromise.fail(new ObjectDoesNotExist(pathID));
			}
			else
			{
				idPromise.complete(project.getString("_id"));
			}
		})
		.onFailure(err -> idPromise.fail(err));		
		return idPromise.future();
	}
}
