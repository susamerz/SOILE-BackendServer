package fi.abo.kogni.soile2.http_server.verticles;


import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.http_server.codeProvider.CodeProvider;
import fi.abo.kogni.soile2.http_server.codeProvider.CompiledCodeProvider;
import fi.abo.kogni.soile2.http_server.codeProvider.JSCodeProvider;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
/**
 * Verticle that handles compiled Code Retrieval.
 * @author Thomas Pfau
 *
 */
public class CodeRetrieverVerticle extends AbstractVerticle {

	static final Logger LOGGER = LogManager.getLogger(CodeRetrieverVerticle.class);

	/**
	 * PsychoPy indicator
	 */
	public final static String PSYCHOPY = "psychopy";
	/**
	 * Elang indicator
	 */
	public final static String ELANG = "elang";
	/**
	 * Qmarkup indicator 
	 */
	public final static String QMARKUP = "qmarkup";
	/**
	 * Javascript indicator
	 */
	public final static String JAVASCRIPT = "javascript";
	private CodeProvider elangProvider;
	private CodeProvider qmarkupProvider;
	private CodeProvider jsProvider;
	
	@Override
	public void start()
	{
		elangProvider = new CompiledCodeProvider(SoileConfigLoader.getVerticleProperty("elangAddress"),vertx.eventBus());
		qmarkupProvider = new CompiledCodeProvider(SoileConfigLoader.getVerticleProperty("questionnaireAddress"),vertx.eventBus());
		jsProvider = new JSCodeProvider(vertx.eventBus());
		LOGGER.debug("Deploying CodeRetriever with id : " + deploymentID());
		vertx.eventBus().consumer(SoileConfigLoader.getVerticleProperty("compilationAddress"), this::compileCode);
		vertx.eventBus().consumer(SoileConfigLoader.getVerticleProperty("gitCompilationAddress"), this::compileGitCode);				
		vertx.eventBus().consumer("soile.tempData.Cleanup", this::cleanUp);		
	}
	
	/**
	 * Clean up data. 
	 * @param cleanUpRequest the request message (just a signal)
	 */
	public void cleanUp(Message<Object> cleanUpRequest)
	{
		elangProvider.cleanUp();
		jsProvider.cleanUp();
		qmarkupProvider.cleanUp();
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void stop(Promise<Void> stopPromise)
	{
		List<Future<Void>> undeploymentFutures = new LinkedList<Future<Void>>();
		undeploymentFutures.add(vertx.eventBus().consumer(SoileConfigLoader.getVerticleProperty("compilationAddress"), this::compileCode).unregister());
		undeploymentFutures.add(vertx.eventBus().consumer(SoileConfigLoader.getVerticleProperty("gitCompilationAddress"), this::compileGitCode).unregister());
		undeploymentFutures.add(vertx.eventBus().consumer("soile.tempData.Cleanup", this::cleanUp).unregister());	
		Future.all(undeploymentFutures).mapEmpty().
		onSuccess(v -> {
			LOGGER.debug("Successfully undeployed CodeRetriever with id : " + deploymentID());
			stopPromise.complete();
		})
		.onFailure(err -> stopPromise.fail(err));			
	}
	
	private void compileCode(Message<JsonObject> sourceCode)
	{
		String type = sourceCode.body().getString("type");
		String code = sourceCode.body().getString("code");
		CodeProvider provider = getProviderForType(type);
		provider.compileCode(code)
		.onSuccess(compiledCode -> {			
			sourceCode.reply(new JsonObject().put("code", compiledCode));
		})
		.onFailure(err -> sourceCode.fail(400, err.getMessage()));
	}
	
	private void compileGitCode(Message<JsonObject> codeLocation)
	{
		JsonObject type = codeLocation.body().getJsonObject("type");
		String id = codeLocation.body().getString("UUID");
		String version = codeLocation.body().getString("version");
		LOGGER.debug(codeLocation.body().encodePrettily());
		CodeProvider provider = getProviderForType(type.getString("language"));
		// this is always a Task object (and we expect the actual ID of the task, so we supplement it with the ID Type to get the correct repository).
		GitFile f = new GitFile("Code.obj", new Task().getTypeID() + id, version);		
		provider.getCode(f)
		.onSuccess(compiledCode -> {			
			codeLocation.reply(new JsonObject().put("code", compiledCode));
		})
		.onFailure(err -> codeLocation.fail(400, err.getMessage()));
	}
	
	
	private CodeProvider getProviderForType(String type)
	{
		switch(type)
		{
			case PSYCHOPY: return jsProvider;
			case ELANG: return elangProvider;
			case QMARKUP: return qmarkupProvider;
			case JAVASCRIPT: return jsProvider;
			default: return null;
		}
	}
	
}
