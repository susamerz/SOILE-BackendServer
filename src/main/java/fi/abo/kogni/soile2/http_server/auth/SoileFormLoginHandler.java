package fi.abo.kogni.soile2.http_server.auth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.impl.UserContextInternal;

/**
 * Form Login handler
 * 
 * @author Thomas Pfau
 *
 */
public class SoileFormLoginHandler extends SoileAuthHandler {
	static final Logger LOGGER = LogManager.getLogger(SoileFormLoginHandler.class);
	private String usernameParam;
	private String passwordParam;
	/**
	 * The AuthenticationProvider for this handler
	 */
	protected final AuthenticationProvider authProvider;
	/**
	 * signal the kind of Multi-Factor Authentication used by the handler
	 */
	protected final String mfa;

	/**
	 * Constructor using no mfa (default)
	 * 
	 * @param authProvider  The {@link AuthenticationProvider}
	 * @param usernameParam username parameter in the form
	 * @param passwordParam password parameter in the form
	 * @param jwtCreator    JWT token creator
	 * @param cookieHandler Handler for Cookies
	 */
	public SoileFormLoginHandler(AuthenticationProvider authProvider, String usernameParam, String passwordParam,
			JWTTokenCreator jwtCreator, SoileCookieCreationHandler cookieHandler) {
		this(authProvider, usernameParam, passwordParam, jwtCreator, cookieHandler, null);

	}

	/**
	 * Constructor using no mfa (default)
	 * 
	 * @param authProvider  The {@link AuthenticationProvider}
	 * @param usernameParam username parameter in the form
	 * @param passwordParam password parameter in the form
	 * @param jwtCreator    JWT token creator
	 * @param cookieHandler Handler for Cookies
	 * @param mfa           signal the kind of Multi-Factor Authentication used by
	 *                      the handler
	 */
	public SoileFormLoginHandler(AuthenticationProvider authProvider, String usernameParam, String passwordParam,
			JWTTokenCreator jwtCreator, SoileCookieCreationHandler cookieHandler, String mfa) {
		super(cookieHandler, jwtCreator);
		this.usernameParam = usernameParam;
		this.passwordParam = passwordParam;
		this.authProvider = authProvider;
		this.mfa = mfa;
	}

	/**
	 * Authenticate the given context
	 * 
	 * @param context context to authenticate (needs to contain the form)
	 * @param handler handler for the authenticated user
	 */
	public void authenticate(RoutingContext context, Handler<AsyncResult<User>> handler) {
		LOGGER.debug("Trying to authenticate a request");
		LOGGER.debug("Request is: " + context.body().asString());
		HttpServerRequest req = context.request();
		if (req.method() != HttpMethod.POST) {
			handler.handle(Future.failedFuture(BAD_METHOD)); // Must be a POST
		} else {
			if (!context.body().available()) {
				handler.handle(Future.failedFuture("BodyHandler is required to process POST requests"));
			} else {
				// this could be a json
				MultiMap params = req.formAttributes();
				String username;
				String password;
				if (params.isEmpty()) {
					// could be a json request
					try {
						JsonObject credentials = context.body().asJsonObject();
						username = credentials.getString("username");
						password = credentials.getString("password");
					} catch (Exception e) {
						handler.handle(Future.failedFuture(new HttpException(401, e.getCause())));
						return;
					}
				} else {
					username = params.get(usernameParam);
					password = params.get(passwordParam);
				}
				if (username == null || password == null) {
					handler.handle(Future.failedFuture(BAD_REQUEST));
				} else {
					authProvider.authenticate(new UsernamePasswordCredentials(username, password)).andThen(authn -> {
						if (authn.failed()) {
							handler.handle(Future.failedFuture(new HttpException(401, authn.cause())));
						} else {
							handler.handle(authn);
						}
					});
				}
			}
		}
	}
}
