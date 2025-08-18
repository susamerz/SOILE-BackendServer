package fi.abo.kogni.soile2.http_server.auth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.http_server.authentication.utils.SoileCookieStrategy;
import fi.abo.kogni.soile2.http_server.authentication.utils.UserUtils;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;

/**
 * This Authentication class offers authentication from soile cookies.
 * @author Thomas Pfau
 *
 */
public class SoileCookieAuth {
	
	private final Vertx vertx;
	private MongoClient client;
	private SoileCookieCreationHandler cookieHandler;
	/**
	 * Logger for the class
	 */
	public static final Logger LOGGER = LogManager.getLogger(SoileCookieAuth.class);
	
	/**
	 * DEfault Constructor
	 * @param vertx the {@link Vertx} instance
	 * @param client the {@link MongoClient} for db access
	 * @param cookieHandler {@link SoileCookieCreationHandler} for cookie building
	 */
	public SoileCookieAuth(Vertx vertx, MongoClient client, SoileCookieCreationHandler cookieHandler)
	{
		this.client = client;
		this.vertx = vertx;
		this.cookieHandler = cookieHandler;
	}
		
	/**
	 * Retrieve a user for the given context if it has a relevant cookie. 
	 * TODO: Ensure that this also works with Tokens. Might not be possible. For Tokens, we probably have to 
	 * @param context the {@link RoutingContext} for the request
	 * @return A {@link Future} of the {@link User} in the context
	 */
	public Future<User> authenticate(RoutingContext context ) {
		LOGGER.debug("Checking session cookies");

		HttpServerRequest request = context.request();		  
		Promise<User> userPromise = Promise.promise();
		if(context.user() == null)
		{ 
			LOGGER.debug(" No user found Checking Cookie");
			Cookie sessionCookie = context.request().getCookie(SoileConfigLoader.getSessionProperty("sessionCookieID"));
			LOGGER.debug(context.request().cookieCount());
			for(Cookie c: context.request().cookies())
			{
				LOGGER.debug(c.toString());
			}
			if(sessionCookie != null)
			{
				// start async operations with a paused request.
				final boolean parseEnded = request.isEnded();
				if (!parseEnded) {
					LOGGER.debug("Pausing Request");
					request.pause();
				}
				try
				{
					String token = SoileCookieStrategy.getTokenFromCookieContent(sessionCookie.getValue());
					String username = SoileCookieStrategy.getUserNameFromCookieContent(sessionCookie.getValue());
					JsonObject command = new JsonObject().put("username", username)
							.put("sessionID", token);

					LOGGER.debug("Trying to validate token:\n" + command.encodePrettily());					
					vertx.eventBus()
					.request("soile.umanager.checkUserSessionValid",command).andThen(
							res ->
					{									
						if(res.succeeded())
						{
							// User token verified, create User and add it to session.
							JsonObject result = (JsonObject)res.result().body();
							if(SoileCommUtils.isResultSuccessFull(result)) 
							{
								JsonObject query = new JsonObject()
										.put(SoileConfigLoader.getUserdbField("usernameField"), username);
								client.find(SoileConfigLoader.getdbProperty("userCollection"), query).andThen( dbRes -> 
								{
									//only resume the request, if we have finished loading everything here.							
									
									if (dbRes.succeeded()) 
									{	
										//retrieve the user from the database entry.
										try
										{
											User user = UserUtils.buildUserFromDBResult(dbRes.result(), username);
											user.principal().put("refreshCookie", true);
											LOGGER.debug("Requesting update of cookie");
											cookieHandler.updateCookie(context, user).onComplete(cookieSaved ->
											{
												LOGGER.debug("Handling returned");
												// only complete the userPromise once the cookie has been stored successfully.
												request.resume();		
												userPromise.complete(user);
											});
											
										}
										catch(Exception e)
										{
											// resume if handling failed
											request.resume();
											LOGGER.error("We found a valid session but could not create the user due to the following error: " + e.toString() );
											userPromise.fail(new HttpException(401));
										}
									}
									else
									{
										// resume if the db Request failed.
										request.resume();
										LOGGER.error("We found a valid session but could not create the user due to the following error");
										LOGGER.error(dbRes.cause());										
										userPromise.fail(new HttpException(401));									}

								});																				
							}
							else
							{
								//Fail if we could not add the user
								LOGGER.error("Session no longer valid , : " + res.cause().getMessage() );
								userPromise.fail(new HttpException(401));							}
						}
						else
						{
							//This would fail since it could not be authenticated.
							LOGGER.error("Session no longer valid , : " + res.cause().getMessage() );
							userPromise.fail(new HttpException(401));						}
					});

				}
				catch(Exception e)
				{
					LOGGER.error("Problem processing cookies: " + e.getMessage() );
					LOGGER.error(e);
					userPromise.fail(new HttpException(401));				}
			}
			else
			{
				LOGGER.debug(" Cookie not found, not authenticated!");
				userPromise.fail(new HttpException(401));
			}

		}
		else
		{
			//TODO: Check whether this is correct, or should not complete with a user.
			userPromise.complete(context.user());
		}	
		return userPromise.future();
	}
	
}
