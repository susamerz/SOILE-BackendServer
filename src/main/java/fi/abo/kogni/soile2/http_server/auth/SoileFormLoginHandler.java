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

	/**
	 * Do post authenticaton steps
	 * @param ctx {@link RoutingContext} for post Auth actions
	 */
	public void postAuthentication(RoutingContext ctx) {
		// get the parameters once more and check, whether to store a cookie.
		User user = ctx.user(); 		
		HttpServerRequest req = ctx.request();
		MultiMap params = req.formAttributes();
		String rememberValue;
		if(params.isEmpty())
		{
			// could be a json request
			try {
				JsonObject credentials = ctx.body().asJsonObject();
				rememberValue = credentials.getString("remember");
			}
			catch (Exception e) {
				rememberValue = null;
			}
		}
		else
		{
			rememberValue = params.get("remember");
		}
		Boolean remember = rememberValue != null ? rememberValue.equals("1") : false;

		LOGGER.debug(user.principal().encodePrettily());
		LOGGER.debug(user.principal().encodePrettily());
		user.principal().put("storeCookie", remember);
		cookieHandler.updateCookie(ctx, user).onComplete(cookieDone ->
		{
			// A response needs to wait on the cookieHandling to be done. Otherwise we can end up with the cookie not stored in the db and a new request failing.
			LOGGER.debug(user.principal().encodePrettily());
			// Finally set the token and send the reply		
			jwtCreator.getToken(ctx).onSuccess(token ->
			{
				ctx.response().setStatusCode(200)
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
				.end(new JsonObject().put("token",token).encode());			
			}).onFailure(fail ->
			{
				if(fail.getCause() instanceof HttpException)
				{
					ctx.fail(((HttpException)fail.getCause()).getStatusCode());
				}
			});
		})
		.onFailure(err -> {
			ctx.fail(500,err);
		});
	}


	@Override
	public void handle(RoutingContext ctx) {

		if (handlePreflight(ctx)) {
			return;
		}

		// pause the request
		if (!ctx.request().isEnded()) {
			ctx.request().pause();
		}

		User user = ctx.user();
		if (user != null) {
			if (mfa != null) {
				// if we're dealing with MFA, the user principal must include a matching mfa
				if (mfa.equals(user.get("mfa"))) {
					// proceed with the router
					if (!ctx.request().isEnded()) {
						ctx.request().resume();
					}
					postAuthentication(ctx);
					return;
				}
			} else {
				// proceed with the router
				if (!ctx.request().isEnded()) {
					ctx.request().resume();
				}
				postAuthentication(ctx);
				return;
			}
		}
		// perform the authentication
		authenticate(ctx, authN -> {
			if (authN.succeeded()) {
				User authenticated = authN.result();
				LOGGER.debug("User added to to context");
				((UserContextInternal)ctx.userContext()).setUser(authenticated);
				Session session = ctx.session();
				if (session != null) {
					// the user has upgraded from unauthenticated to authenticated
					// session should be upgraded as recommended by owasp
					session.regenerateId();
				}
				// proceed with the router
				if (!ctx.request().isEnded()) {
					ctx.request().resume();
				}
				postAuthentication(ctx);
			} else {
				// to allow further processing if needed
				Throwable cause = authN.cause();
				if (!ctx.request().isEnded()) {
					ctx.request().resume();
				}
				processException(ctx, cause);
			}
		});
	}

	/**
	 * This method is protected so custom auth handlers can override the default
	 * error handling
	 * @param ctx The Routing context in which an exceptio happened
	 * @param exception the Exception that occured
	 */
	protected void processException(RoutingContext ctx, Throwable exception) {
		if (exception != null) {
			if (exception instanceof HttpException) {
				final int statusCode = ((HttpException) exception).getStatusCode();
				final String payload = ((HttpException) exception).getPayload();

				switch (statusCode) {
				case 302:
					ctx.response()
					.putHeader(HttpHeaders.LOCATION, payload)
					.setStatusCode(302)
					.end("Redirecting to " + payload + ".");
					return;
				case 401:
					/*if (!"XMLHttpRequest".equals(ctx.request().getHeader("X-Requested-With"))) {
	              setAuthenticateHeader(ctx);
	            }*/
					ctx.fail(401, exception);
					return;
				default:
					ctx.fail(statusCode, exception);
					return;
				}
			}
		}

		// fallback 500
		ctx.fail(exception);
	}

	private boolean handlePreflight(RoutingContext ctx) {
		final HttpServerRequest request = ctx.request();
		// See: https://www.w3.org/TR/cors/#cross-origin-request-with-preflight-0
		// Preflight requests should not be subject to security due to the reason UAs will remove the Authorization header
		if (request.method() == HttpMethod.OPTIONS) {
			// check if there is a access control request header
			final String accessControlRequestHeader = ctx.request().getHeader(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS);
			if (accessControlRequestHeader != null) {
				// lookup for the Authorization header
				for (String ctrlReq : accessControlRequestHeader.split(",")) {
					if (ctrlReq.equalsIgnoreCase("Authorization")) {
						// this request has auth in access control, so we can allow preflighs without authentication
						ctx.next();
						return true;
					}
				}
			}
		}

		return false;
	}

}
