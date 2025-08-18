package fi.abo.kogni.soile2.http_server;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.aalto.scicomp.gitFs.gitProviderVerticle;
import fi.abo.kogni.soile2.http_server.verticles.GitManagerVerticle;
import fi.abo.kogni.soile2.http_server.verticles.PermissionVerticle;
import fi.abo.kogni.soile2.http_server.verticles.SoileUserManagementVerticle;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PfxOptions;

/**
 * The Soile Server Verticle. 
 * Sets up the configuration, 
 * Initializes all other necessary Verticles and starts the web-server.
 * @author Thomas Pfau
 *
 */
public class SoileServerVerticle extends AbstractVerticle {

	static final Logger LOGGER = LogManager.getLogger(SoileServerVerticle.class);
	private JsonObject soileConfig = new JsonObject();
	SoileRouteBuilding soileRouter;
	ConcurrentLinkedQueue<String> deployedVerticles;
	
	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		soileRouter = new SoileRouteBuilding();
		deployedVerticles = new ConcurrentLinkedQueue<>();		
		
		setupConfig() // As the very first stepp we need to set up the config so that it is available for all later steps.
		.compose(this::setupFolders) // Set up all necessary folders		
		.compose(this::deployVerticles) // deploy all necessary verticles (including routing etc)
		.compose(this::startHttpServer) // start the web server.
		.onComplete(res ->
		{
			if(res.succeeded())
			{
				LOGGER.debug("Server started successfully and listening on port " + SoileConfigLoader.getServerIntProperty("port"));
				
				startPromise.complete();
			}
			else
			{
				LOGGER.debug("Error starting server " + res.cause().getMessage());
				LOGGER.error(res.cause(), res.cause());
				startPromise.fail(res.cause().getMessage());
			}
		});
	}
	@Override
	public void stop(Promise<Void> stopPromise) throws Exception {
		LOGGER.debug("Stopping Server Verticle");					
		undeploy(stopPromise);
	}	
	
	/**
	 * Helper function to undeploy this verticle. Will undeploy all verticles created by this verticle, if they are still deployed
	 * @param stopPromise the promise to complete when stopped successfully
	 * @throws Exception thrown exceptions
	 */
	public void undeploy(Promise<Void> stopPromise) throws Exception {
		LOGGER.debug("Stopping Server Verticle");
		
		@SuppressWarnings("rawtypes")
		List<Future<Void>> unDeploymentFutures = new LinkedList<Future<Void>>();		
		for(String deploymentID : deployedVerticles)
		{
			LOGGER.debug("Trying to undeploy : " + deploymentID);
			unDeploymentFutures.add(undeploy(deploymentID));
		}
		//deploymentFutures.add(Future.<String>future(promise -> vertx.deployVerticle("js:templateManager.js", opts, promise)));
		Future.all(unDeploymentFutures).mapEmpty()
		.onSuccess(v -> {			
			stopPromise.complete();			
		})
		.onFailure(err -> {
			LOGGER.debug("Could not stop all child verticles");
			stopPromise.complete();
		});						
	}	
	// This ALWAYS eturns a succeeded future. 
	private Future<Void> undeploy(String deploymentID) {
		Promise<Void> undeployedFuture = Promise.promise();
		vertx.undeploy(deploymentID).onFailure(err -> {
			LOGGER.debug("Couldn't undeploy " + deploymentID);
			if(err.getMessage().equals("Unk.complete()nown deployment"))
			{
				LOGGER.debug("Already undeployed");
				undeployedFuture.complete();
			}
			else
			{
				undeployedFuture.fail(err);
			}
						
		}).onSuccess(res -> {
			LOGGER.debug("Successfully undeployed " + deploymentID);
			undeployedFuture.complete();			
		});
		
		return undeployedFuture.future();
	}
	
	Future<String> addDeployedVerticle(Future<String> result, String target)
	{
		result.onSuccess(deploymentID -> {
			LOGGER.debug("Deploying verticle " + target  + " with id:  " + deploymentID );
			deployedVerticles.add(deploymentID);
		})
		.onFailure(err -> {			
			LOGGER.error("Failed to start Verticle: " + target);
			LOGGER.error(err,err);
		});
		return result;
	}
	
	Future<Void> deployVerticles(Void unused)
	{
		DeploymentOptions opts = new DeploymentOptions().setConfig(soileConfig);
		soileRouter.setDeploymentOptions(opts);
		@SuppressWarnings("rawtypes")
		List<Future<String>> deploymentFutures = new LinkedList<Future<String>>();
		deploymentFutures.add(addDeployedVerticle(vertx.deployVerticle(new SoileUserManagementVerticle(), opts), "UserManagement"));
		deploymentFutures.add(addDeployedVerticle(vertx.deployVerticle(soileRouter, opts), "Router"));
		deploymentFutures.add(addDeployedVerticle(vertx.deployVerticle(new gitProviderVerticle(SoileConfigLoader.getServerProperty("gitVerticleAddress"), SoileConfigLoader.getServerProperty("soileGitFolder"),LOGGER.getLevel()), opts ), "Git"));
		deploymentFutures.add(addDeployedVerticle(vertx.deployVerticle(new GitManagerVerticle(), opts ), "GitManager"));
		deploymentFutures.add(addDeployedVerticle(vertx.deployVerticle(new PermissionVerticle(), opts ), "Permissions"));
		return Future.all(deploymentFutures).mapEmpty();
	}
	
	Future<Void> setupConfig()
	{
		Promise<Void> finishedSetupPromise = Promise.promise();		
		SoileConfigLoader.setupConfig(vertx)
		.onSuccess(finished -> {
			soileConfig.mergeIn(SoileConfigLoader.config());
			LOGGER.debug("Configuration loaded");
			finishedSetupPromise.complete();
		})
		.onFailure(err -> finishedSetupPromise.fail(err));
		
		return finishedSetupPromise.future();		
	}
	

	Future<Void> setupFolders(Void unused)
	{
		return createFolder(SoileConfigLoader.getServerProperty("soileGitDataLakeFolder"))
		.compose( unusedVoid -> {
		 return createFolder(SoileConfigLoader.getServerProperty("soileResultDirectory"));	
		});		
	}

	Future<Void> createFolder(String folderName)
	{
		Promise<Void> folderCreatedPromise = Promise.<Void>promise();
		vertx.fileSystem().exists(folderName).onSuccess(exists-> {
			if(!exists)
			{
				
				vertx.fileSystem().mkdirs(folderName)
				.onSuccess(created -> {
					LOGGER.debug("Folder " + folderName +  " created.");
					folderCreatedPromise.complete();
				})
				.onFailure(err -> {
					folderCreatedPromise.fail(err);
				});				
			}
			else
			{
				LOGGER.debug("Folder " + folderName +  " existed.");
				folderCreatedPromise.complete();	
			}
		}).onFailure(fail ->{
			folderCreatedPromise.fail(fail);
		});
		return folderCreatedPromise.future();
	}
	
	
	
	Future<Void> startHttpServer(Void unused)
	{
		
		JsonObject http_config = soileConfig.getJsonObject("http_server");
		HttpServerOptions opts = new HttpServerOptions()
									 .setLogActivity(true);
		LOGGER.debug("Starting HTTP Server");
		
		if(SoileConfigLoader.getServerBooleanProperty("useSSL", false))
		{
			LOGGER.debug("Using HTTPS");	
			
			String sslStoreFile = SoileConfigLoader.getServerProperty("sslStoreFile");
			opts.setSsl(true);
			if(sslStoreFile.endsWith(".p12"))
			{
			PfxOptions keyOptions = new PfxOptions()					
			.setPath(SoileConfigLoader.getServerProperty("sslStoreFile"))
				.setPassword(SoileConfigLoader.getServerProperty("sslSecret"))
				.setAlias("soile2");
				opts.setKeyCertOptions(keyOptions);				
			}
			if(sslStoreFile.endsWith(".pem"))
			{
				PemKeyCertOptions keyOptions = new PemKeyCertOptions()
													.setCertPath(SoileConfigLoader.getServerProperty("sslStoreFile"))
													.setKeyPath(SoileConfigLoader.getServerProperty("sslSecret"));
				opts.setKeyCertOptions(keyOptions);
			}
		
		}
		
		HttpServer server = vertx.createHttpServer(opts).requestHandler(soileRouter.getRouter());
		int httpPort = http_config.getInteger("port", SoileConfigLoader.getServerIntProperty("port"));			
		return server.listen(httpPort).onSuccess(started -> { LOGGER.debug("Http Server started listening");}).mapEmpty();	
	}
}
