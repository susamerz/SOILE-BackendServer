package fi.abo.kogni.soile2.http_server.auth;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.http_server.authentication.utils.UserUtils;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

/**
 * Class that provides Authentication for token carrying requests.
 * 
 * @author Thomas Pfau
 *
 */
public class PasswordReset extends SoileAuthHandler {
	static final Logger LOGGER = LogManager.getLogger(PasswordReset.class);

	private MongoClient client;
	private Vertx vertx;
	private SoileCookieCreationHandler cookieHandler;
	private int resettime = 60 * 60 * 1000; // 1 hour ttl for reset tokens.

	/**
	 * Default constructor
	 * 
	 * @param client        a Mongo client to retrieve data from the db
	 * @param tokenCreator  a jwt Token Creator for auth purposes
	 * @param vertx         the Vertx Instance
	 * @param cookieHandler a cookie handler for authentication purposes
	 */
	public PasswordReset(MongoClient client, JWTTokenCreator tokenCreator, Vertx vertx,
			SoileCookieCreationHandler cookieHandler) {
		super(cookieHandler, tokenCreator);
		this.client = client;
		this.vertx = vertx;
	}

	private Future<JsonObject> getUserData(String userNameOrEmail) {
		Promise<JsonObject> promise = Promise.<JsonObject>promise();
		UserUtils.getUserDataFromCollection(client, userNameOrEmail, res -> {
			res.onSuccess(userData -> {
				promise.complete(userData);
			})
					.onFailure(err -> promise.fail(err));
		});
		return promise.future();
	}

	private Future<Void> checkAndCleanUpTokenForUser(JsonObject userData) {
		Promise<Void> promise = Promise.<Void>promise();
		String tokenCollection = SoileConfigLoader.getdbProperty("tokenCollection");
		String username = userData.getString(SoileConfigLoader.getUserdbField("usernameField"));
		String usernameField = SoileConfigLoader.getUserdbField("usernameField");
		LOGGER.debug("Checking for existing tokens for user.");
		this.client.find(tokenCollection, new JsonObject().put(usernameField, username))
				.onSuccess(res -> {
					// a token already exists
					if (res.size() > 0) {
						JsonObject item = res.get(0);
						if ((System.currentTimeMillis() - item.getInteger("timestamp")) > resettime) {
							this.client.removeDocuments(tokenCollection, userData)
									.onSuccess(done -> promise.complete())
									.onFailure(err -> {
										LOGGER.error("Unable to clean up tokens");
										LOGGER.error(err);
										promise.fail(err);
									});
						} else {
							promise.fail("Token Already exists");
						}

					} else {
						// nothing found, great. Just return
						promise.complete();
					}
				});

		return promise.future();
	}

	private void generateUniqueToken(Promise<String> fut, JsonObject userData) {
		// First, check, if there is already a recent request for this user
		// we will not generate multiple tokens for a user, thus making sure
		// that you cannot spam a user with reset mails
		String username = userData.getString(SoileConfigLoader.getUserdbField("usernameField"));
		String usernameField = SoileConfigLoader.getUserdbField("usernameField");
		String tokenCollection = SoileConfigLoader.getdbProperty("tokenCollection");
		LOGGER.debug("Trying to check for token");
		this.checkAndCleanUpTokenForUser(userData)
				.onSuccess(yay -> {
					java.security.SecureRandom random = new java.security.SecureRandom();
					byte[] tokenBytes = new byte[64];
					random.nextBytes(tokenBytes);
					String token = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
					JsonObject query = new JsonObject().put("token", token);
					LOGGER.debug("Nothing found for user, trying to generate token");
					this.client.find(tokenCollection, query).onComplete(ar -> {
						LOGGER.debug("Trying to retrieve message user data");
						if (ar.succeeded() && (ar.result() == null || ar.result().isEmpty())) {
							LOGGER.debug("Saving token.");
							query.put(usernameField, username);
							query.put("timestamp", System.currentTimeMillis());
							client.save(tokenCollection, query)
									.onSuccess(done -> {
										fut.complete(token);
									})
									.onFailure(err -> {
										fut.fail(err);
									});

						} else {
							// Token exists, generate a new one and try again
							LOGGER.debug("Retrying.");
							generateUniqueToken(fut, userData);
						}
					});
				})
				.onFailure(err -> {
					LOGGER.error(err);
					fut.fail(err.getMessage());
				});
	}

	/**
	 * Test auth route
	 * 
	 * @param ctx the routing context
	 */
	public void request_reset(RoutingContext ctx) {
		RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
		JsonObject body = params.body().getJsonObject();
		String emailOrUserName = body.getString("emailOrUserName");
		LOGGER.debug("Received Password Reset request");
		createOneTimeLoginAndSendMail(emailOrUserName)
				.onFailure(err -> {
					LOGGER.error(err.getMessage());
					LOGGER.warn("Got a invalid request for a password reset for user " + emailOrUserName);
				})
				.onComplete(done -> {
					LOGGER.debug("Request handled, returning");
					// In any case we will respond with "request received" and not give out further
					// information.
					ctx.response().putHeader("content-type", "text/plain");
					ctx.response().end("Request received");
				});

	}

	public Future<Void> createOneTimeLoginAndSendMail(String userNameOrEmail) {
		Promise<Void> promise = Promise.<Void>promise();
		Promise<String> token_promise = Promise.<String>promise();
		LOGGER.debug("Trying to retrieve message user data");
		this.getUserData(userNameOrEmail)
				.onSuccess(userData -> {
					LOGGER.debug("Got user data");
					this.generateUniqueToken(token_promise, userData);
					token_promise.future().onSuccess(token -> {
						String email = userData.getString(SoileConfigLoader.getUserdbField("userEmailField"));
						if (email == null) {
							promise.fail("No Email found");
							return;
						}
						sendMail(email, token)
								.onSuccess(res -> promise.complete())
								.onFailure(err -> promise.fail(err));
					})
							.onFailure(err -> promise.fail(err));
				})
				.onFailure(err -> promise.fail(err.getMessage()));
		return promise.future();
	}

	private Future<User> getUserForToken(String authToken) {

		Promise<User> userPromise = Promise.<User>promise();
		var query = new JsonObject().put("token", authToken);
		client.findOneAndDelete(SoileConfigLoader.getdbProperty("tokenCollection"), query)
				.onSuccess(res -> {
					String userName = res.getString(SoileConfigLoader.getUserdbField("usernameField"));
					if (userName == null) {
						userPromise.fail("No username in token");
						return;
					}
					Long timestamp = res.getLong("timestamp");
					if (timestamp == null || (System.currentTimeMillis() - timestamp) > resettime) {
						userPromise.fail("Token expired");
						return;
					}
					UserUtils.getUserDataFromCollection(client, userName, dbresult -> {
						dbresult.onSuccess(userData -> {
							User user = UserUtils.buildUserForDBEntry(userData, userName);
							userPromise.complete(user);
						})
								.onFailure(err -> userPromise.fail(err));

					});
				})
				.onFailure(err -> userPromise.fail(err));
		return userPromise.future();
	}

	@Override
	public void authenticate(RoutingContext context, Handler<AsyncResult<User>> handler) {
		HttpServerRequest request = context.request();
		String token = request.getParam("token");
		LOGGER.debug("Trying to authenticate a user with one time token " + token);
		if (token == null) {
			handler.handle(Future.failedFuture("Missing Token"));
		} else {
			getUserForToken(token)
					.onSuccess(user -> {
						LOGGER.debug("User obtained ");
						handler.handle(Future.succeededFuture(user));
					})
					.onFailure(err -> handler.handle(Future.failedFuture(err)));
		}
	}

	private Future<Void> sendMail(String mailAddress, String token) {
		
		return this.vertx.executeBlocking(() -> {
				String domain = SoileConfigLoader.getServerProperty("domain");
				String subject = "SOILE Password Reset";
				String from = "no-reply@" + domain;
				String content = "Here is your one time login link for SOILE that you can use for a password reset.\n"
						+ "https://" + domain + "/password_reset?token=" + token + "\n\n"
						+ "Yours,\nSOILE Team\n";
				Properties props = new Properties();
				props.put("mail.smtp.host", "localhost"); // we always send via the local mail server
				props.put("mail.smtp.port", 25);
				props.put("mail.smtp.auth", "false");
				props.put("mail.smtp.starttls.enable", "false");
				LOGGER.debug("Mailing to: " + mailAddress);
				Session session = Session.getInstance(props);
				Message message = new MimeMessage(session);
				message.setFrom(new InternetAddress(from, "SOILE Server Password Reset"));
				message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mailAddress));
				message.setSubject(subject);
				message.setText(content);
				Transport.send(message);
				return null;
			
		});

	}

	@Override
	public void postAuthentication(RoutingContext ctx) {
		// get the parameters once more and check, whether to store a cookie.
		User user = ctx.user();
		LOGGER.debug("Post Authentication");
		LOGGER.debug(user.principal().encodePrettily());
		LOGGER.debug(user.principal().encodePrettily());
		// Finally set the token and send the reply
		jwtCreator.getToken(ctx).onSuccess(token -> {
			LOGGER.debug("Token created, returning");
			ctx.response().setStatusCode(200)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
					.end(new JsonObject().put("token", token).encode());
		}).onFailure(fail -> {
			LOGGER.debug("Token creation failed");
			if (fail.getCause() instanceof HttpException) {
				ctx.fail(((HttpException) fail.getCause()).getStatusCode());
			}
		});

	}
}
