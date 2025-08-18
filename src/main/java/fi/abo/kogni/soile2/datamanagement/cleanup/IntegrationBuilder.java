package fi.abo.kogni.soile2.datamanagement.cleanup;

import java.util.HashSet;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentLinkedDeque;

import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Experiment;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Project;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * A Class that creates integrations between different parts of the Projects
 * @author Thomas Pfau
 *
 */
public class IntegrationBuilder {
	private static final Logger LOGGER = LogManager.getLogger(IntegrationBuilder.class);

	ElementManager<Task> taskManager;
	ElementManager<Experiment> experimentManager;
	ElementManager<Project> projectManager;
	MongoClient client;
	
	
	/**
	 * Default constructor
	 * @param taskManager the {@link ElementManager} for {@link Task}s to use 
	 * @param experimentManager the {@link ElementManager} for {@link Experiment}s to use
	 * @param projectManager the {@link ElementManager} for {@link Project}s to use
	 * @param client the {@link MongoClient} connecting to the database
	 */
	public IntegrationBuilder(ElementManager<Task> taskManager, ElementManager<Experiment> experimentManager,
			ElementManager<Project> projectManager, MongoClient client) {
		super();
		this.taskManager = taskManager;
		this.experimentManager = experimentManager;
		this.projectManager = projectManager;
		this.client = client;
	}

	Future<Void> addDependencies()
	{
		Promise<Void> donePromise = Promise.promise();
		Promise<Void> experimentsPromise = Promise.promise();
		Promise<Void> projectsPromise = Promise.promise();
		LinkedList<Future<Void>> allDone = new LinkedList<>();
		allDone.add(experimentsPromise.future());
		allDone.add(projectsPromise.future());
		experimentManager.getElementList(true)		
		.onSuccess(expList -> {
			LinkedList<Future<Void>> expDone = new LinkedList<>();
			for(int i = 0 ; i < expList.size(); i++)
			{				
				expDone.add(handleExperiment(expList.getJsonObject(i).getString("UUID")));
			}
			Future.all(expDone)
			.onSuccess(done -> {
				experimentsPromise.complete();
			})
			.onFailure(err -> experimentsPromise.fail(err));
					
		});
		
		projectManager.getElementList(true)
		.onSuccess(projList -> {
			LinkedList<Future<Void>> projDone = new LinkedList<>();
			for(int i = 0 ; i < projList.size(); i++)
			{
				projDone.add(handleProject(projList.getJsonObject(i).getString("UUID")));
			}
			Future.all(projDone)
			.onSuccess(done -> {
				projectsPromise.complete();
			})
			.onFailure(err -> projectsPromise.fail(err));
		
		});
		Future.all(allDone)
		.onSuccess(allFinished -> {
			donePromise.complete();
		})
		.onFailure(err -> donePromise.fail(err));
				
		return donePromise.future();

	}

	Future<Void> handleExperiment(String UUID)
	{
		Promise<Void> elementDone = Promise.promise();											
		experimentManager.getVersionListForElement(UUID)
		.onSuccess(versionList -> {
			LinkedList<Future<Void>> versionsDone = new LinkedList<>();
			// we only need to care about taggable versions
			ConcurrentLinkedDeque<String> experimentIDs = new ConcurrentLinkedDeque<>();
			ConcurrentLinkedDeque<String> taskIDs = new ConcurrentLinkedDeque<>();			
			for(int i = 0; i < versionList.size(); ++i)
			{				
				JsonObject currentVersion = versionList.getJsonObject(i);
				if(currentVersion.getBoolean("canbetagged", false) || currentVersion.containsKey("tag"))
				{
					// is taggable
					versionsDone.add( experimentManager.getAPIElementFromDB(UUID, currentVersion.getString("version"))							
							.compose(apiExperiment -> {
								JsonObject expData = apiExperiment.getGitJson();
								JsonArray elements = expData.getJsonArray("elements");
								for(int j = 0; j < elements.size(); j++)
								{
									JsonObject current = elements.getJsonObject(j);							
									switch(current.getString("elementType"))
									{
									case "task" : taskIDs.add(current.getJsonObject("data").getString("UUID")); break;
									case "experiment" : experimentIDs.add(current.getJsonObject("data").getString("UUID")); break;
									default: continue;
									}
								}
								return Future.succeededFuture();
							}).onFailure(err -> {
								LOGGER.error("Failed building for " + UUID + " @ " + currentVersion.getString("version"));
								LOGGER.error(err,err);
							})
							.mapEmpty()
							);										
				}
			}
			Future.all(versionsDone)
			.compose(finished -> {
				return experimentManager.getElement(UUID);
			})
			.compose(exp -> {
				HashSet<String> uniqueTasks = new HashSet<>();
				uniqueTasks.addAll(taskIDs);
				HashSet<String> uniqueExperiments = new HashSet<>();
				uniqueExperiments.addAll(experimentIDs);
				exp.setDependencies(new JsonObject()
										.put("tasks", new JsonArray(new LinkedList<String>(uniqueTasks)))
										.put("experiments", new JsonArray(new LinkedList<String>(uniqueExperiments))));					
				return exp.save(client);
			})
			.onSuccess(saved -> {
				LOGGER.info("Finished dependencies for" + UUID );
				elementDone.complete();								
			})
			.onFailure(err -> elementDone.fail(err));			
		});
			
		return elementDone.future();
	}

	Future<Void> handleProject(String UUID)
	{
		
		Promise<Void> elementDone = Promise.promise();											
		projectManager.getVersionListForElement(UUID)
		.onSuccess(versionList -> {
			LinkedList<Future<Void>> versionsDone = new LinkedList<>();
			// we only need to care about taggable versions
			ConcurrentLinkedDeque<String> experimentIDs = new ConcurrentLinkedDeque<>();
			ConcurrentLinkedDeque<String> taskIDs = new ConcurrentLinkedDeque<>();			
			for(int i = 0; i < versionList.size(); ++i)
			{				
				JsonObject currentVersion = versionList.getJsonObject(i);
				if(currentVersion.getBoolean("canbetagged", false) || currentVersion.containsKey("tag"))
				{
					// is taggable
					versionsDone.add( projectManager.getAPIElementFromDB(UUID, currentVersion.getString("version"))
							.onFailure(err -> {
								LOGGER.error("Failed building for " + UUID + " @ " + currentVersion.getString("version"));
								LOGGER.error(err,err);
							})
							.compose(apiProject-> {
								JsonObject projData = apiProject.getGitJson();

								JsonArray taskElements = projData.getJsonArray("tasks");
								JsonArray experimentElements = projData.getJsonArray("experiments");										
								for(int j = 0; j < experimentElements.size(); j++)
								{
									experimentIDs.add(experimentElements.getJsonObject(j).getString("UUID"));									
								}
								for(int j = 0; j < taskElements.size(); j++)
								{
									taskIDs.add(taskElements.getJsonObject(j).getString("UUID"));									
								}
								return Future.succeededFuture();
							}).mapEmpty());								
				}
			}
			Future.all(versionsDone)
			.compose(finished -> {
				return projectManager.getElement(UUID);
			})
			.compose(exp -> {
				HashSet<String> uniqueTasks = new HashSet<>();
				uniqueTasks.addAll(taskIDs);
				HashSet<String> uniqueExperiments = new HashSet<>();
				uniqueExperiments.addAll(experimentIDs);
				exp.setDependencies(new JsonObject()
										.put("tasks", new JsonArray(new LinkedList<String>(uniqueTasks)))
										.put("experiments", new JsonArray(new LinkedList<String>(uniqueExperiments))));					
				return exp.save(client);
			})
			.onSuccess(saved -> {
				LOGGER.error("Finished dependencies for " + UUID );
				elementDone.complete();								
			})
			.onFailure(err -> elementDone.fail(err));			
		});
			
		return elementDone.future();
	}

}
