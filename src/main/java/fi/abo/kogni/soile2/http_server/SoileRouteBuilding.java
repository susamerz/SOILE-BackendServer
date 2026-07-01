package fi.abo.kogni.soile2.http_server;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeResourceManager;
import fi.abo.kogni.soile2.elang.verticle.ExperimentLanguageVerticle;
import fi.abo.kogni.soile2.http_server.auth.JWTTokenCreator;
import fi.abo.kogni.soile2.http_server.auth.PasswordReset;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthentication;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthenticationBuilder;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization;
import fi.abo.kogni.soile2.http_server.auth.SoileCookieCreationHandler;
import fi.abo.kogni.soile2.http_server.auth.SoileFormLoginHandler;
import fi.abo.kogni.soile2.http_server.auth.SoileSessionHandler;
import fi.abo.kogni.soile2.http_server.requestHandling.IDSpecificFileProvider;
import fi.abo.kogni.soile2.http_server.routes.ElementRouter;
import fi.abo.kogni.soile2.http_server.routes.ParticipationRouter;
import fi.abo.kogni.soile2.http_server.routes.ProjectRouter;
import fi.abo.kogni.soile2.http_server.routes.SoileRouter;
import fi.abo.kogni.soile2.http_server.routes.StudyRouter;
import fi.abo.kogni.soile2.http_server.routes.TaskRouter;
import fi.abo.kogni.soile2.http_server.routes.UserRouter;
import fi.abo.kogni.soile2.http_server.verticles.CodeRetrieverVerticle;
import fi.abo.kogni.soile2.http_server.verticles.DataBundleGeneratorVerticle;
import fi.abo.kogni.soile2.http_server.verticles.ParticipantVerticle;
import fi.abo.kogni.soile2.http_server.verticles.TaskInformationverticle;
import fi.abo.kogni.soile2.projecthandling.apielements.APIExperiment;
import fi.abo.kogni.soile2.projecthandling.apielements.APIProject;
import fi.abo.kogni.soile2.projecthandling.participant.ParticipantHandler;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Experiment;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Project;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.impl.StudyHandler;
import fi.abo.kogni.soile2.qmarkup.verticle.QuestionnaireRenderVerticle;
import fi.abo.kogni.soile2.utils.DebugRouter;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.ChainAuthHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.DigestAuthHandler;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.ext.web.openapi.router.impl.RouterBuilderImpl;
import io.vertx.ext.web.sstore.LocalSessionStore;
import io.vertx.openapi.contract.OpenAPIContract;
import io.vertx.openapi.validation.RequestUtils;
/**
 * Verticle that handles Route Building for the Soile Backend Platform
 * 
 * @author Thomas Pfau
 *
 */
public class SoileRouteBuilding extends AbstractVerticle {

	private static final Logger LOGGER = LogManager.getLogger(SoileRouteBuilding.class);

	private MongoClient client;
	private SoileCookieCreationHandler cookieHandler;
	private Router soileRouter;
	private SoileAuthenticationBuilder handler;
	private SoileAuthorization soileAuthorization;
	private DataLakeResourceManager resourceManager;
	private ParticipantHandler partHandler;
	private StudyHandler projHandler;
	private TaskRouter taskRouter;
	private ParticipationRouter partRouter;
	private IDSpecificFileProvider fileProvider;
	DeploymentOptions soileOpts;
	AuthenticationHandler anyAuth;
	AuthenticationHandler userAuth;
	@SuppressWarnings("rawtypes")
	private List<MessageConsumer> consumers;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		consumers = new LinkedList<>();
		cookieHandler = new SoileCookieCreationHandler(vertx.eventBus());
		this.client = MongoClient.createShared(vertx, SoileConfigLoader.getMongoCfg(), "ROUTER_MONGO");
		resourceManager = new DataLakeResourceManager(vertx);
		soileAuthorization = new SoileAuthorization(client);
		projHandler = new StudyHandler(client, vertx);
		partHandler = new ParticipantHandler(client, projHandler, vertx);
		fileProvider = new IDSpecificFileProvider(resourceManager);
		LOGGER.debug("Starting Routerbuilder");
		deployVerticles()
				.compose(this::createRouter)
				.compose(this::setupAuth)
				.compose(this::setupLogin)
				.compose(this::addHandlers)
				.compose(this::setupTaskAPI)
				.compose(this::setupExperimentAPI)
				.compose(this::setupProjectAPI)
				.compose(this::setupStudyAPI)
				.compose(this::setupParticipationAPI)
				.compose(this::setupUserAPI)
				.onSuccess(routerBuilder -> {
					// add Debug, Logger and Session Handlers.
					soileRouter = routerBuilder.createRouter();
					// as unfortunate as this is, we need to build a few routes manually, as they
					// cannot be matched properly at the moment
					setUpSpecialRoutes(soileRouter);
					// now, add the cleanup callBack for the different Routers, which will cache
					// data.
					consumers.add(vertx.eventBus().consumer("soile.tempData.Cleanup", this::cleanUP));
					soileRouter.errorHandler(401, ErrorHandler.create(vertx));
					soileRouter.errorHandler(403, ErrorHandler.create(vertx));
					startPromise.complete();
				})
				.onFailure(fail -> {
					LOGGER.error("Failed Starting router with error:", fail);
					startPromise.fail(fail);
				});

	}

	@Override
	@SuppressWarnings("rawtypes")
	public void stop(Promise<Void> stopPromise) {
		soileRouter.clear();
		List<Future<Void>> undeploymentFutures = new LinkedList<Future<Void>>();
		for(MessageConsumer consumer : consumers)
		{
			undeploymentFutures.add(consumer.unregister().mapEmpty());
		}			
		
		Future.all(undeploymentFutures).mapEmpty().
		onSuccess(v -> stopPromise.complete())
		.onFailure(err -> {
			LOGGER.error("Couldn't undeploy all child verticles");
			stopPromise.complete();
		});		
	}

	/**
	 * Deploy the required verticles.
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private Future<Void> deployVerticles()
	{
		List<Future<String>> deploymentFutures = new LinkedList<Future<String>>();
		vertx.deployVerticle(new ExperimentLanguageVerticle(SoileConfigLoader.getVerticleProperty("elangAddress")), soileOpts);
		vertx.deployVerticle(new QuestionnaireRenderVerticle(SoileConfigLoader.getVerticleProperty("questionnaireAddress")), soileOpts);
		vertx.deployVerticle(new CodeRetrieverVerticle(), soileOpts);
		vertx.deployVerticle(new ParticipantVerticle(partHandler, projHandler), soileOpts);
		vertx.deployVerticle(new TaskInformationverticle(), soileOpts);
		vertx.deployVerticle(new DataBundleGeneratorVerticle(client,projHandler,partHandler), soileOpts);
		return Future.all(deploymentFutures).mapEmpty();

	}

	
	public void setDeploymentOptions(DeploymentOptions opts) {
	}

	/**
	 * Create the router from the API file defined in the config.
	 * 
	 * @param unused an unused Void input for composition.
	 * @return A Future of the {@link RouterBuilder} created
	 */
	private Future<RouterBuilderImpl> createRouter(Void unused)
	{
		LOGGER.debug(config().getString("api"));
		Future<OpenAPIContract> contract = OpenAPIContract.from(vertx, config().getString("api"));		
		return contract.compose(openapi -> Future.succeededFuture(
				new RouterBuilderImpl(vertx,openapi, 
						(routingContext, operation) -> RequestUtils.extract(routingContext.request(), operation))
				));
	}

	/**
	 * Get the router
	 * 
	 * @return the {@link Router}
	 */
	public Router getRouter() {
		return this.soileRouter;
	}

	/**
	 * Clean up old cached data.
	 * 
	 * @param cleanupRequest A message (could be void)
	 */
	public void cleanUP(Message<Object> cleanupRequest) {
		partHandler.cleanup();
		projHandler.cleanup();
		taskRouter.cleanup();
	}

	/**
	 * Set up auth handling
	 * 
	 * @param builder the Routerbuilder to be used.
	 * @return the routerbuilder in a future for composite use
	 */
	Future<RouterBuilder> setupAuth(RouterBuilderImpl builder)
	{	
		handler = new SoileAuthenticationBuilder();
		
		JWTAuthHandler JWTAuth =  JWTAuthHandler.create(handler.getJWTAuthProvider(vertx));
		AuthenticationHandler tokenAuth =  handler.getTokenAuthProvider(partHandler, client);
		AuthenticationHandler cookieAuth =  handler.getCookieAuthProvider(vertx, client, cookieHandler);
				
		builder.security("cookieAuth", cookieAuth,null);			  
		builder.security("JWTAuth",JWTAuth,null);			
		builder.security("tokenAuth", tokenAuth, null);
		
		anyAuth = ChainAuthHandler.any().add(JWTAuth).add(cookieAuth).add(tokenAuth);
		userAuth = ChainAuthHandler.any().add(JWTAuth).add(cookieAuth);
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	Future<RouterBuilder> addHandlers(RouterBuilder builder) {
		if (LOGGER.getLevel() == Level.DEBUG) { // we add the router if debugging
			builder.rootHandler(LoggerHandler.create());
		}
		builder.rootHandler(new SoileSessionHandler(LocalSessionStore.create(vertx)));
		// TODO: Make flexible and set up for all front-end components
		CorsHandler cors = CorsHandler.create()
				.allowedMethod(HttpMethod.POST)
				.allowedMethod(HttpMethod.GET)
				.allowedMethod(HttpMethod.OPTIONS)
				.allowCredentials(true)
				.allowedHeader("Access-Control-Allow-Headers")
				.allowedHeader("Authorization")
				.allowedHeader("Access-Control-Allow-Method")
				.allowedHeader("Access-Control-Allow-Origin")
				.allowedHeader("Access-Control-Allow-Credentials")
				.allowedHeader("Content-Type");
		JsonArray corsHosts = SoileConfigLoader.getConfig(SoileConfigLoader.HTTP_SERVER_CFG).getJsonArray("corsURLS",
				new JsonArray());
		for (int i = 0; i < corsHosts.size(); ++i) {
			cors.addOrigin(corsHosts.getString(i));
		}
		builder.rootHandler(cors);
		builder.rootHandler(BodyHandler.create());
		builder.rootHandler(new DebugRouter());
		return Future.<RouterBuilder>succeededFuture(builder);
	}
	
	Future<RouterBuilder> setupLogin(RouterBuilder builder)
	{
		
		SoileFormLoginHandler formLoginHandler = new SoileFormLoginHandler(new SoileAuthentication(client), "username", "password",new JWTTokenCreator(handler,vertx), cookieHandler);		
		builder.getRoute("loginUser").addHandler(formLoginHandler::handle);
		builder.getRoute("testAuth").addHandler(this::testAuth);
		builder.getRoute("logout").addHandler(this::logout);
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	private void logout(RoutingContext ctx) {
		User currentUser = ctx.user();
		if (currentUser != null) {
			ctx.session().destroy();
			cookieHandler.invalidateSessionCookie(ctx)
					.onSuccess(loggedOut -> {
						ctx.response().setStatusCode(200)
								.end();
					})
					.onFailure(err -> SoileRouter.handleError(err, ctx));
		}
	}

	/**
	 * Test auth route
	 * 
	 * @param ctx the routing context
	 */
	public void testAuth(RoutingContext ctx) {
		LOGGER.debug("AuthTest got a request");
		if (ctx.user() != null) {
			LOGGER.debug(ctx.user());
			LOGGER.debug(ctx.user().principal().encodePrettily());
			LOGGER.debug(ctx.user().attributes().encodePrettily());
			LOGGER.debug(ctx.user().authorizations().toString());
			ctx.request().response()
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("authenticated", true)
							.put("user", ctx.user().principal().getString("username"))
							.put("roles",
									ctx.user().principal().getValue(SoileConfigLoader.getSessionProperty("userRoles")))
							.encodePrettily());
		} else {
			ctx.request().response()
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("authenticated", false).put("user", null).encodePrettily());
		}
	}

	/**
	 * Everything that needs to be done for the Task API
	 * 
	 * @param builder
	 * @return
	 */
	private Future<RouterBuilder> setupTaskAPI(RouterBuilder builder)
	{
		taskRouter = new TaskRouter( client, fileProvider, vertx, soileAuthorization);
		builder.getRoute("getTaskList").addHandler(taskRouter::getElementList);
		builder.getRoute("getVersionsForTask").addHandler(taskRouter::getVersionList);
		builder.getRoute("createTask").addHandler(taskRouter::create);
		builder.getRoute("getTask").addHandler(taskRouter::getElement);
		builder.getRoute("getTaskFileList").addHandler(taskRouter::getTaskFileList);
		builder.getRoute("updateTask").addHandler(taskRouter::writeElement);
		builder.getRoute("compileCode").addHandler(taskRouter::compileCode);
		builder.getRoute("getExecution").addHandler(taskRouter::getCompiledTask);
		builder.getRoute("getTagForTaskVersion").addHandler(taskRouter::getTagForVersion);
		builder.getRoute("downloadTask").addHandler(taskRouter::downloadTask);
		builder.getRoute("uploadTask").addHandler(taskRouter::uploadTask);
		builder.getRoute("removeTagsForTask").addHandler(taskRouter::removeTagsFromElement);
		builder.getRoute("addTagToTaskVersion").addHandler(taskRouter::addTagToVersion);
		builder.getRoute("deleteTask").addHandler(taskRouter::deleteElement);
		builder.getRoute("getCodeOptions").addHandler(taskRouter::getCodeOptions);		
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	/**
	 * Everything that needs to be done for the Experiment API
	 * 
	 * @param builder
	 * @return
	 */
	private Future<RouterBuilder> setupExperimentAPI(RouterBuilder builder)
	{
		ElementRouter<Experiment> router = new ElementRouter<Experiment>(new ElementManager<Experiment>(Experiment::new, APIExperiment::new, client, vertx), soileAuthorization, vertx.eventBus(), client );
		builder.getRoute("getExperimentList").addHandler(router::getElementList);
		builder.getRoute("getVersionsForExperiment").addHandler(router::getVersionList);
		builder.getRoute("createExperiment").addHandler(router::create);
		builder.getRoute("getExperiment").addHandler(router::getElement);
		builder.getRoute("updateExperiment").addHandler(router::writeElement);
		builder.getRoute("getTagForExperimentVersion").addHandler(router::getTagForVersion);
		builder.getRoute("removeTagsForExperiment").addHandler(router::removeTagsFromElement);
		builder.getRoute("deleteExperiment").addHandler(router::deleteElement);
		builder.getRoute("addTagToExperimentVersion").addHandler(router::addTagToVersion);
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	/**
	 * Everything that needs to be done for the Project API
	 * 
	 * @param builder
	 * @return
	 */
	private Future<RouterBuilder> setupProjectAPI(RouterBuilder builder)
	{
		ProjectRouter router = new ProjectRouter(new ElementManager<Project>(Project::new, APIProject::new, client, vertx), soileAuthorization, vertx.eventBus(), client );
		builder.getRoute("getProjectList").addHandler(router::getElementList);
		builder.getRoute("getVersionsForProject").addHandler(router::getVersionList);
		builder.getRoute("createProject").addHandler(router::create);
		builder.getRoute("getProject").addHandler(router::getElement);
		builder.getRoute("updateProject").addHandler(router::writeElement);
		builder.getRoute("getTagForProjectVersion").addHandler(router::getTagForVersion);
		builder.getRoute("isFilterValid").addHandler(router::isFilterValid);
		builder.getRoute("removeTagsForProject").addHandler(router::removeTagsFromElement);
		builder.getRoute("deleteProject").addHandler(router::removeTagsFromElement);
		builder.getRoute("addTagToProjectVersion").addHandler(router::addTagToVersion);
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	/**
	 * Everything that needs to be done for the project execution API
	 * The Project execution API will NOT use shortcuts!
	 * 
	 * @param builder
	 * @return
	 */
	private Future<RouterBuilder> setupStudyAPI(RouterBuilder builder) {
		StudyRouter router = new StudyRouter(soileAuthorization, vertx, client, partHandler, projHandler);
		builder.getRoute("listDownloadData").addHandler(router::listDownloadData);
		builder.getRoute("startStudy").addHandler(router::startProject);
		builder.getRoute("getRunningStudies").addHandler(router::getRunningProjectList);
		builder.getRoute("stopStudy").addHandler(router::stopProject);		
		builder.getRoute("restartStudy").addHandler(router::startStudy);
		builder.getRoute("deleteStudy").addHandler(router::deleteProject);
		builder.getRoute("getStudyResults").addHandler(router::getProjectResults);		
		builder.getRoute("downloadResults").addHandler(router::downloadResults);
		builder.getRoute("downloadTest").addHandler(router::downloadTest);
		builder.getRoute("createTokens").addHandler(router::createTokens);
		builder.getRoute("resetStudy").addHandler(router::resetStudy);
		builder.getRoute("updateStudy").addHandler(router::updateStudy);
		builder.getRoute("getStudyProperties").addHandler(router::getStudyProperties);
		builder.getRoute("getTokenInformation").addHandler(router::getTokenInformation);
		builder.getRoute("getCollaboratorsForStudy").addHandler(router::getCollaboratorsForStudy);		
		builder.getRoute("getStudyList").addHandler(router::getStudyList);		
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	/**
	 * Everything that needs to be done for the project execution API
	 * 
	 * @param builder
	 * @return
	 */
	private Future<RouterBuilder> setupParticipationAPI(RouterBuilder builder) {
		partRouter = new ParticipationRouter(soileAuthorization, vertx, client, partHandler, projHandler, fileProvider);
		builder.getRoute("submitResults").addHandler(context -> {partRouter.handleRequest(context,partRouter::submitResults);});
		builder.getRoute("getTaskInfo").addHandler(context -> {partRouter.handleRequest(context,partRouter::getTaskInfo);});
		builder.getRoute("runTask").addHandler(context -> {partRouter.handleRequest(context,partRouter::runTask);});
		builder.getRoute("getID").addHandler(context -> {partRouter.handleRequest(context,partRouter::getID);});
		builder.getRoute("signUpForProject").addHandler(context -> {partRouter.handleRequest(context,partRouter::signUpForProject);});
		builder.getRoute("withdrawFromStudy").addHandler(context -> {partRouter.handleRequest(context,partRouter::withdrawFromStudy);});
		builder.getRoute("uploadData").addHandler(context -> {partRouter.handleRequest(context,partRouter::uploadData);});
		builder.getRoute("getPersistentData").addHandler(context -> {partRouter.handleRequest(context,partRouter::getPersistentData);});		
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	/**
	 * Everything that needs to be done for the User API
	 * 
	 * @param builder
	 * @return
	 */
	private Future<RouterBuilder> setupUserAPI(RouterBuilder builder) {
		UserRouter router = new UserRouter(soileAuthorization, vertx, client);
		builder.getRoute("registerUser").addHandler(router::registerUser);
		builder.getRoute("listUsers").addHandler(router::listUsers);
		builder.getRoute("createUser").addHandler(router::createUser);
		builder.getRoute("removeUser").addHandler(router::removeUser);
		builder.getRoute("getUserInfo").addHandler(router::getUserInfo);
		builder.getRoute("setUserInfo").addHandler(router::setUserInfo);
		builder.getRoute("setPassword").addHandler(router::setPassword);
		builder.getRoute("setRole").addHandler(router::setRole);
		builder.getRoute("permissionChange").addHandler(router::permissionChange);
		builder.getRoute("permissionOrRoleRequest").addHandler(router::permissionOrRoleRequest);
		builder.getRoute("getUserActiveProjects").addHandler(router::getUserActiveProjects);
		return Future.<RouterBuilder>succeededFuture(builder);
	}

	/**
	 * These are a few special routes which unfortunately cannot be
	 * mapped directly from OPenAPI, as they require that all sub-pathes be properly
	 * matched.
	 * 
	 * @param router
	 */
	private void setUpSpecialRoutes(Router router) {
		// Order is important here! the /lib/* needs
		// to match before /* because /* matches any /*/lib route as well and libs need
		// to be loaded from the lib dir.
		// This actually means we need to reject any resource upload that starts with
		// /lib!
		router.route(HttpMethod.GET, "/run/:id/:taskID/lib/*").handler(anyAuth).handler(context -> {
			partRouter.handleRequest(context, partRouter::getLib);
		});
		router.route(HttpMethod.GET, "/run/:id/:taskID/*").handler(anyAuth).handler(context -> {
			partRouter.handleRequest(context, partRouter::getResourceForExecution);
		});
		router.route(HttpMethod.GET, "/task/:id/:version/resource/*").handler(userAuth)
				.handler(taskRouter::getResource);
		router.route(HttpMethod.POST, "/task/:id/:version/resource/*").handler(userAuth)
				.handler(taskRouter::putResource);
		router.route(HttpMethod.GET, "/task/:id/:version/execute").handler(userAuth)
				.handler(taskRouter::getCompiledTask);
		router.route(HttpMethod.GET, "/task/:id/:version/execute/lib/*").handler(userAuth).handler(taskRouter::getLib);
		router.route(HttpMethod.GET, "/task/:id/:version/execute/*").handler(userAuth)
				.handler(taskRouter::getResourceForExecution);
		router.route(HttpMethod.POST, "/task/:id/:version/resource/*").handler(userAuth)
				.handler(taskRouter::putResource);
	}

}
