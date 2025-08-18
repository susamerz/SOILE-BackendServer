package fi.abo.kogni.soile2.http_server.verticles;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.aalto.scicomp.gitFs.gitProviderVerticle;
import fi.abo.kogni.soile2.datamanagement.git.GitElement;
import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.datamanagement.git.GitManager;
import fi.abo.kogni.soile2.datamanagement.utils.DataRetrieverImpl;
import fi.abo.kogni.soile2.datamanagement.utils.TimeStampedMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Verticle that handles Git Acces via the GitManager (essentially this is a "frontEnd" for the GitManager class)
 * This verticle keeps some data in memory for faster response time.
 * @author Thomas Pfau
 *
 */
public class GitManagerVerticle extends AbstractVerticle{


	EventBus eb;
	GitManager gitManager;
	TimeStampedMap<GitFile, String> resourceStringContents;
	TimeStampedMap<GitFile, JsonObject> resourceJsonContents;
	TimeStampedMap<GitFile, JsonObject> fileJsonContents;
	TimeStampedMap<GitFile, String> fileStringContents;
	TimeStampedMap<GitElement, JsonArray> resourceLists;
	TimeStampedMap<GitElement, JsonArray> historyLists;
	
	private List<MessageConsumer> consumers;
	private static final Logger LOGGER = LogManager.getLogger(GitManagerVerticle.class);

	@Override
	public void start()
	{
		eb = vertx.eventBus();
		gitManager = new GitManager(eb);
		consumers = new LinkedList<>();
		resourceStringContents = new TimeStampedMap<GitFile,String>(new DataRetrieverImpl<GitFile,String>(gitManager::getGitResourceContents), 3600*2);
		resourceJsonContents = new TimeStampedMap<GitFile,JsonObject>(new DataRetrieverImpl<GitFile,JsonObject>(gitManager::getGitResourceContentsAsJson), 3600*2);
		fileJsonContents = new TimeStampedMap<GitFile,JsonObject>(new DataRetrieverImpl<GitFile,JsonObject>(gitManager::getGitFileContentsAsJson), 3600*2);
		fileStringContents = new TimeStampedMap<GitFile,String>(new DataRetrieverImpl<GitFile,String>(gitManager::getGitFileContents), 3600*2);
		resourceLists = new TimeStampedMap<GitElement,JsonArray>(new DataRetrieverImpl<GitElement,JsonArray>(gitManager::getResourceList), 3600*2);
		historyLists = new TimeStampedMap<GitElement,JsonArray>(new DataRetrieverImpl<GitElement,JsonArray>(gitManager::getHistory), 3600*2);
		
		
		consumers.add(eb.consumer("soile.git.doesRepoExist",this::doesRepoExist));
		consumers.add(eb.consumer("soile.git.doesRepoAndVersionExist",this::doesRepoAndVersionExist));
		consumers.add(eb.consumer("soile.git.initRepo",this::initRepo));
		consumers.add(eb.consumer("soile.git.getGitFileContents",this::getGitFileContents));
		consumers.add(eb.consumer("soile.git.getGitResourceContents",this::getGitResourceContents));
		consumers.add(eb.consumer("soile.git.getGitResourceContentsAsJson",this::getGitResourceContentsAsJson));
		consumers.add(eb.consumer("soile.git.getGitFileContentsAsJson",this::getGitFileContentsAsJson));
		consumers.add(eb.consumer("soile.git.getResourceList",this::getResourceList));
		consumers.add(eb.consumer("soile.git.getHistory",this::getHistory));
		consumers.add(eb.consumer("soile.git.writeGitFile",this::writeGitFile));
		consumers.add(eb.consumer("soile.git.writeGitResourceFile",this::writeGitResourceFile));
		consumers.add(eb.consumer("soile.git.deleteGitResourceFile",this::deleteGitResourceFile));
		consumers.add(eb.consumer("soile.git.cleanUp",this::cleanUp));
		consumers.add(eb.consumer("soile.git.deleteRepo",this::deleteRepo));
	}
	
	
	@Override
	public void stop(Promise<Void> stopPromise)
	{
		List<Future<Void>> undeploymentFutures = new LinkedList<Future<Void>>();
		for(MessageConsumer consumer : consumers)
		{
			undeploymentFutures.add(consumer.unregister());
		}				
		Future.all(undeploymentFutures).mapEmpty().
		onSuccess(v -> {
			LOGGER.debug("Successfully undeployed SoileUserManager with id : " + deploymentID());
			stopPromise.complete();
		})
		.onFailure(err -> stopPromise.fail(err));			
	}

	
	/**
	 * Test whether a repo element exists asynchronosly
	 * @param message the message with the "elementID" to check
	 */
	public void cleanUp(Message<String> message)
	{
		resourceStringContents.cleanUp();
		resourceJsonContents.cleanUp();
		fileJsonContents.cleanUp();
		fileStringContents.cleanUp();
		resourceLists.cleanUp();
		message.reply(true);
	}
	
	/**
	 * Test whether a repo element exists asynchronosly
	 * @param message the message with the "elementID" to check
	 */
	public void doesRepoAndVersionExist(Message<JsonObject> message)
	{
		gitManager.doesRepoAndVersionExist(new GitElement(message.body()))
		.onSuccess(exist -> {			
			message.reply(exist);
		})
		.onFailure(err -> message.fail(500, err.getMessage()));
	}

	/**
	 * Test whether a repo element exists asynchronosly
	 * @param message the message with the "elementID" to check
	 */
	public void doesRepoExist(Message<String> message)
	{
		gitManager.doesRepoExist(message.body())
		.onSuccess(exist -> {
			message.reply(exist);
		})
		.onFailure(err -> message.fail(500, err.getMessage()));
	}

	/**
	 * Initialize a repository
	 * @param message the message with the "elementID" to create 
	 * The supplied message gets answered with the current Version of the (empty) repository. Fails if the repository exists.    
	 */
	public void initRepo(Message<String> message)
	{
		gitManager.initRepo(message.body())
		.onSuccess(version -> {
			message.reply(version);
		})
		.onFailure(err -> message.fail(500, err.getMessage()));
	}

	/**
	 * Initialize a repository
	 * @param message the message with the "elementID" to create 
	 * The supplied message gets answered with the current Version of the (empty) repository. Fails if the repository exists.    
	 */
	public void deleteRepo(Message<String> message)
	{
		gitManager.deleteRepo(message.body())
		.onSuccess(deleted -> {
			message.reply(deleted);
		})
		.onFailure(err -> message.fail(500, err.getMessage()));
	}
	
	/**
	 * Get the file contents of a file in the github repository, these are all just json/linker files).
	 * @param request the message with the GitFile contents in JsonObject format and that will be replied to.  
	 */
	public void getGitFileContents(Message<JsonObject> request)
	{
		fileStringContents.getData(new GitFile(request.body()))		
		.onSuccess(contents -> {
			request.reply(contents);
		})
		.onFailure(err -> handleFail(request,err));
	}


	/**
	 * Get the file contents of a resource file in the github repository as string, 
	 * since no binary files are in the git repo and those are all pointers this will essentially return the pointer file.
	 * @param request the message with the GitFile contents and that will be replied to. 
	 */
	public void getGitResourceContents(Message<JsonObject> request)
	{
		resourceStringContents.getData(new GitFile(request.body()))
		.onSuccess(contents -> {
			request.reply(contents);
		})
		.onFailure(err -> handleFail(request,err));
	}
	/**
	 * Get the file contents of a git resource file as a Json Object. the reply will be the json object of the contents. 
	 * @param request The message containing the {@link JsonObject} indicating the GitFile and which will be replied to with the {@link JsonObject} of the fiel contents
	 */
	public void getGitResourceContentsAsJson(Message<JsonObject> request)
	{
		resourceJsonContents.getData(new GitFile(request.body()))
		.onSuccess(contents -> {
			request.reply(contents);
		})
		.onFailure(err -> handleFail(request,err));
	}

	/**
	 * Get the file contents of a git file as a Json Object. 
     * @param request The message containing the {@link JsonObject} indicating the GitFile and which will be replied to with the {@link JsonObject} of the fiel contents	 
     */
	public void getGitFileContentsAsJson(Message<JsonObject> request)
	{
		fileJsonContents.getData(new GitFile(request.body()))
		.onSuccess(contents -> {
			request.reply(contents);
		})
		.onFailure(err -> handleFail(request,err));
	}
	/**
	 * Get the resource files available for a specific git Version of an Object as a JsonArray
	 * @param request The message containing the {@link JsonObject} indicating the GitElement for which to retrieve the resource list and which to reply to.  
	 */
	public void getResourceList(Message<JsonObject> request)
	{
		LOGGER.debug("Querying resource list from gitManager");
		resourceLists.getData(new GitElement(request.body()))
		.onSuccess(fileList -> {
			request.reply(fileList);
		})
		.onFailure(err -> handleFail(request,err));
	}
	/**
	 * Get the history (i.e. all previous commits) for a given git version.  
	 * @param request The message containing the {@link JsonObject} indicating the GitElement and to which to reply with the history
	 */
	public void getHistory(Message<JsonObject> request)
	{
		LOGGER.debug("Querying resource list from gitManager");
		historyLists.getData(new GitElement(request.body()))
		.onSuccess(historyList -> {
			request.reply(historyList);
		})
		.onFailure(err -> handleFail(request,err));
	}
	/**
	 * Write data to a file specified by the {@link GitFile}, reply with the new version of the respective repo. 
	 * a request with gitFile fields and a data field for the data to write.
	 * @param request The message containing the {@link JsonObject} indicating the GitFile and which to reply to 
	 */
	public void writeGitFile(Message<JsonObject> request)
	{
		try {
			LOGGER.debug(request.body().encodePrettily());
			gitManager.writeGitFile(new GitFile(request.body()), request.body().getJsonObject("data"), request.body().getString("tag",null))
			.onSuccess(version -> {
				request.reply(version);
			})
			.onFailure(err -> handleFail(request,err));
		}
		catch(ClassCastException e)
		{
			gitManager.writeGitFile(new GitFile(request.body()), request.body().getString("data"), request.body().getString("tag",null))
			.onSuccess(version -> {
				request.reply(version);
			})
			.onFailure(err -> handleFail(request,err));
		}
	}

	/**
	 * Write data to a file specified by the {@link GitFile} in the resources folder of the repo.
	 * @param request The message containing the {@link JsonObject} indicating the GitFile and content and which to reply to
	 */
	public void writeGitResourceFile(Message<JsonObject> request)
	{
		try {
			gitManager.writeGitResourceFile(new GitFile(request.body()), request.body().getJsonObject("data"))
			.onSuccess(version -> {
				request.reply(version);
			})
			.onFailure(err -> handleFail(request,err));
		}
		catch(ClassCastException e)
		{
			// This happens, if "data is nto a JsonObject but a string."
			gitManager.writeGitResourceFile(new GitFile(request.body()), request.body().getString("data"))
			.onSuccess(version -> {
				request.reply(version);
			})
			.onFailure(err -> handleFail(request,err));
		}
	}

	/**
	 * Delete the specified git Resource file  
	 * @param request The message containing the {@link JsonObject} indicating the GitFile and content and which to reply to   
	 */
	public void deleteGitResourceFile(Message<JsonObject> request)
	{
		gitManager.deleteGitResourceFile(new GitFile(request.body()))
		.onSuccess(version -> {
			request.reply(version);
		})
		.onFailure(err -> handleFail(request,err));
	}
	
	private void handleFail(Message request, Throwable err)
	{
		if( err instanceof ReplyException)
		{
			int statusCode = ((ReplyException) err).failureCode();
			int returnCode = 400; 
			switch(statusCode)
			{
				case gitProviderVerticle.NO_VERSION: returnCode = 400; break;
				case gitProviderVerticle.INVALID_REQUEST: returnCode = 400; break;
				case gitProviderVerticle.BRANCH_DOES_NOT_EXIST: returnCode = 404; break;
				case gitProviderVerticle.REPO_DOES_NOT_EXIST: returnCode = 404; break;
				case gitProviderVerticle.FILE_DOES_NOT_EXIST_FOR_VERSION: returnCode = 404; break;
				case gitProviderVerticle.REPO_ALREADY_EXISTS: returnCode = 409; break;
				case gitProviderVerticle.GIT_ERROR: returnCode = 500; break;
				case gitProviderVerticle.FOLDER_NOT_DIRECTORY: returnCode = 400; break;
				case gitProviderVerticle.REPO_NOT_DELETED: returnCode = 500; break;						
			}
			request.fail(returnCode, err.getMessage());
		}
		else
		{
			request.fail(500, err.getMessage());
		}
	}
}


