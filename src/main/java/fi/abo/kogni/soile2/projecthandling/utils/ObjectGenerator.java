package fi.abo.kogni.soile2.projecthandling.utils;

import java.io.File;
/**
 * THIS CLASS IS NOT INTENDED TO BE USED IN THE FINAL PRODUCT, IT IS A HELPER CLASS FOR TESTS TO CREATE OBJECTS
 */
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.http_server.requestHandling.SOILEUpload;
import fi.abo.kogni.soile2.projecthandling.apielements.APIExperiment;
import fi.abo.kogni.soile2.projecthandling.apielements.APIProject;
import fi.abo.kogni.soile2.projecthandling.apielements.APITask;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Experiment;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Project;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.MimeMapping;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * Helper Class to generate a couple of Objects (mainly for testing)
 * @author Thomas Pfau
 *
 */
public class ObjectGenerator {

	private static final Logger LOGGER = LogManager.getLogger(ObjectGenerator.class);

	
	/**
	 * Build a API Task 
	 * @param manager The {@link ElementManager} to use
	 * @param elementID the ID of the element
	 * @return a {@link Future} of A {@link APITask}
	 */
	public static Future<APITask> buildAPITask(ElementManager<Task> manager, String elementID)
	{
		return buildAPITask(manager, elementID, null);
	}
	
	/**
	 * Build an APIexperiment
	 * @param experimentManager the {@link ElementManager} for Experiments
	 * @param taskManager the {@link ElementManager} for Tasks
	 * @param client the {@link MongoClient} for db access
	 * @param experimentName the name of the experiment
	 * @return A {@link Future} of an {@link APIExperiment}
	 */
	public static Future<APIExperiment> buildAPIExperiment(ElementManager<Experiment> experimentManager,ElementManager<Task> taskManager,MongoClient client, String experimentName)
	{
		return buildAPIExperiment(experimentManager, taskManager, client, experimentName, null);
	}

	
	/**
	 * Same as buildAPIExperiment but also allowing to set a path to the data of the element
	 * @param experimentManager the {@link ElementManager} for Experiments
	 * @param taskManager the {@link ElementManager} for Tasks
	 * @param client the {@link MongoClient} for db access
	 * @param experimentName the name of the experiment
	 * @param datapath the data path
	 * @return A {@link Future} of an {@link APIExperiment}
	 */
	@SuppressWarnings("rawtypes")	
	public static Future<APIExperiment> buildAPIExperiment(ElementManager<Experiment> experimentManager,ElementManager<Task> taskManager,MongoClient client, String experimentName, String datapath)
	{
		Promise<APIExperiment> experimentPromise = Promise.promise();
		try
		{
			JsonObject ExperimentDef;			
			if(datapath == null)
			{
				ExperimentDef = new JsonObject(Files.readString(Paths.get(ObjectGenerator.class.getClassLoader().getResource("APITestData/ExperimentData.json").getPath()))).getJsonObject(experimentName);			
			}
			else
			{
				ExperimentDef = new JsonObject(Files.readString(Paths.get(datapath, "APITestData/ExperimentData.json"))).getJsonObject(experimentName);			
			}
			
			APIExperiment apiExperiment = new APIExperiment();
			apiExperiment.setPrivate(ExperimentDef.getBoolean("private"));
			experimentManager.createOrLoadElement(ExperimentDef.getString("name"))
			.onSuccess(experiment -> {	
				experiment.setPrivate(apiExperiment.getPrivate());
				apiExperiment.setName(experiment.getName());
				apiExperiment.setVersion(experiment.getCurrentVersion());
				apiExperiment.setUUID(experiment.getUUID());				

				ConcurrentHashMap<String, JsonObject> elements = new ConcurrentHashMap<String, JsonObject>();
				List<Future<Void>> partFutures = new LinkedList<Future<Void>>();
				for(Object item : ExperimentDef.getJsonArray("items"))
				{
					JsonObject current = (JsonObject) item;
					if(current.getString("type").equals("task"))
					{
						partFutures.add(
								buildAPITask(taskManager, current.getString("name"), datapath).onSuccess(task -> {
									experiment.addElement(task.getUUID());
									JsonObject taskInstance = new JsonObject();
									taskInstance.put("instanceID", current.getString("instanceID"))
									.put("next", current.getString("next", "end"))
									.put("position", current.getValue("position"))
									.put("UUID", task.getUUID())
									.put("version", task.getVersion())
									.put("filter", current.getString("filter",""))
									.put("name", task.getName())
									.put("codeType", task.getCodeType())
									.put("outputs", current.getJsonArray("outputs",new JsonArray()));
									elements.put(current.getString("instanceID"), new JsonObject().put("elementType", "task")
											.put("data",taskInstance));
								}).mapEmpty()
								);
					}
					if(current.getString("type").equals("filter"))
					{
						elements.put(current.getString("instanceID"), 
									 new JsonObject().put("elementType", "filter")
									 				 .put("position", current.getValue("position"))
									 				 .put("data",current.getJsonObject("data")
									 				 .put("instanceID", current.getString("instanceID"))));
					}					
					if(current.getString("type").equals("experiment"))
					{
						partFutures.add(
								buildAPIExperiment(experimentManager, taskManager, client, current.getString("name"), datapath).onSuccess(subexperiment -> {
									experiment.addElement(subexperiment.getUUID());
									JsonObject experimentInstance = new JsonObject();
									experimentInstance.put("instanceID", current.getString("instanceID"))
									.put("position", current.getValue("position"))
									.put("next", current.getString("next", "end"))
									.put("UUID", subexperiment.getUUID())
									.put("version", subexperiment.getVersion())
									.put("randomize", current.getBoolean("randomize",false))
									.put("name", subexperiment.getName());
									elements.put(current.getString("instanceID"), 
											new JsonObject().put("elementType", "experiment")
											.put("data", experimentInstance));
								}).mapEmpty()
								);

					}
				}
				//deploymentFutures.add(Future.<String>future(promise -> vertx.deployVerticle("js:templateManager.js", opts, promise)));
				Future.all(partFutures).mapEmpty().onSuccess(Void -> {
					// once all is done, we put it in in the right order.
					for(Object item : ExperimentDef.getJsonArray("items"))
					{
						JsonObject current = (JsonObject) item;

						apiExperiment.getElements().add(elements.get(current.getString("instanceID")));
					}					
					experimentManager.updateElement(apiExperiment,"Initial_Version")
					.onSuccess(newVersion -> { 
						apiExperiment.setVersion(newVersion);
						
						experimentPromise.complete(apiExperiment);
					})
					.onFailure(err -> {
						experimentPromise.fail(err);
					});

				})
				.onFailure(err -> {
					experimentPromise.fail(err);
				});
			})
			.onFailure(fail -> experimentPromise.fail(fail));
		}
		catch(IOException e)
		{
			experimentPromise.fail(e);
		}
		return experimentPromise.future();
	}

	/**
	 * A  utility to generate API Projects
	 * @param projectManager the {@link ElementManager} for {@link Project}s
	 * @param expManager the {@link ElementManager} for Experiments
	 * @param taskManager the {@link ElementManager} for Tasks
	 * @param client the {@link MongoClient} for db access 
	 * @param projectName the name of the project
	 * @return A Future of a {@link APIProject}
	 */
	public static Future<APIProject> buildAPIProject(ElementManager<Project> projectManager, 
			 ElementManager<Experiment> expManager,
			 ElementManager<Task> taskManager, 
			 MongoClient client, String projectName
			 )
	{
		return buildAPIProject(projectManager, expManager, taskManager, client, projectName, null);
	}

	
	/**
	 * Same as buildAPIProject but with the option to set a datapath
	 * @param projectManager the {@link ElementManager} for {@link Project}s
	 * @param expManager the {@link ElementManager} for Experiments
	 * @param taskManager the {@link ElementManager} for Tasks
	 * @param client the {@link MongoClient} for db access
 	 * @param projectName the name of the project
	 * @param datapath the data path
	 * @return A Future of a {@link APIProject}
	 */
	@SuppressWarnings("rawtypes")
	public static Future<APIProject> buildAPIProject(ElementManager<Project> projectManager, 
													 ElementManager<Experiment> expManager,
													 ElementManager<Task> taskManager, 
													 MongoClient client, String projectName,
													 String datapath)
	{
		Promise<APIProject> projectPromise = Promise.promise();
		try
		{
			
			JsonObject projectDef;			
			if(datapath == null)
			{
				projectDef = new JsonObject(Files.readString(Paths.get(ObjectGenerator.class.getClassLoader().getResource("APITestData/ProjectData.json").getPath()))).getJsonObject(projectName);			
			}
			else
			{
				projectDef = new JsonObject(Files.readString(Paths.get(datapath, "APITestData/ProjectData.json"))).getJsonObject(projectName);			
			}		
			APIProject apiProject = new APIProject();		
			apiProject.setStart(projectDef.getString("start"));
			projectManager.createOrLoadElement(projectDef.getString("name"))
			.onSuccess(project -> {							
				apiProject.setName(project.getName());
				apiProject.setVersion(project.getCurrentVersion());
				apiProject.setUUID(project.getUUID());							
				ConcurrentHashMap<String, JsonObject> tasks = new ConcurrentHashMap<String, JsonObject>();
				List<Future<Void>> taskFutures = new LinkedList<Future<Void>>();
				for(Object item : projectDef.getJsonArray("tasks"))
				{
					JsonObject current = (JsonObject) item;
					taskFutures.add(
							buildAPITask(taskManager, current.getString("name"), datapath)
							.onSuccess(task -> {
								project.addElement(task.getUUID());
								JsonObject taskInstance = new JsonObject();
								taskInstance.put("instanceID", current.getString("instanceID"))
								.put("position", current.getValue("position"))
								.put("next", current.getString("next", "end"))
								.put("UUID", task.getUUID())
								.put("version", task.getVersion())
								.put("filter", current.getString("filter",""))
								.put("name", task.getName())
								.put("codeType", task.getCodeType())
								.put("outputs", current.getJsonArray("outputs",new JsonArray()));
								tasks.put(current.getString("name"), taskInstance);
							}).mapEmpty()
							);
				}
				Future.all(taskFutures).mapEmpty().onSuccess(Void -> {
					LinkedList<JsonObject> taskList = new LinkedList<JsonObject>();
					taskList.addAll(tasks.values());
					JsonArray taskArray = new JsonArray(taskList);
					apiProject.setTasks(taskArray);
					// and now we do the experiments. Since they could in theory refer back to the same unique tasks, we need to have created the tasks first.
					ConcurrentHashMap<String, JsonObject> experiments = new ConcurrentHashMap<String, JsonObject>();
					List<Future<Void>> experimentFutures = new LinkedList<Future<Void>>();
					// and for filters. This should work even without
					JsonArray filters = apiProject.getFilters();
					for(Object item : projectDef.getJsonArray("filters", new JsonArray()))
					{
						filters.add(item);
					}
					for(Object item : projectDef.getJsonArray("experiments"))
					{
						JsonObject current = (JsonObject) item;
						experimentFutures.add(
								buildAPIExperiment(expManager, taskManager, client, current.getString("name"), datapath)
								.onSuccess(experiment -> {
									project.addElement(experiment.getUUID());						
									JsonObject expinstance = experiment.getAPIJson();
									expinstance.put("instanceID",current.getString("instanceID"))
											   .put("position", current.getValue("position"))
											   .put("next", current.getString("next", "end"))
											   .put("random", current.getBoolean("random", true));
									updateExperimentElementTargets(expinstance);
									experiments.put(current.getString("name"), expinstance);								
								}).mapEmpty()
								);						
					}
					// once the futures are set up, wait for them do be done and then finish up.
					Future.all(experimentFutures).mapEmpty()
					.onSuccess(Void2 -> {
						LinkedList<JsonObject> expList = new LinkedList<JsonObject>();
						expList.addAll(experiments.values());
						JsonArray expArray = new JsonArray(expList);
						apiProject.setExperiments(expArray);						
						projectManager.updateElement(apiProject, "Initial_Version")
						.onSuccess( newVersion -> {
								apiProject.setVersion(newVersion);
								//apiProject.setTag("Initial_Version");
								// Saving the associated project.								
								projectPromise.complete(apiProject);															
							})
							.onFailure(err -> projectPromise.fail(err));
						})
						.onFailure(err -> projectPromise.fail(err));
					

				})
				.onFailure(err -> projectPromise.fail(err));

				//deploymentFutures.add(Future.<String>future(promise -> vertx.deployVerticle("js:templateManager.js", opts, promise)));

			})
			.onFailure(fail -> projectPromise.fail(fail));
		}
		catch(IOException e)
		{
			projectPromise.fail(e);
		}
		return projectPromise.future();
	}
	
	/**
	 * Create a Project
	 * @param client {@link MongoClient} for db access
	 * @param vertx {@link Vertx} instance for communication
	 * @param projectName name of the project
	 * @return A {@link Future} of a {@link JsonObject} representing the Project
	 */
	public static Future<JsonObject> createProject(MongoClient client, Vertx vertx, String projectName)
	{
		return createProject(client, vertx, projectName, null);
	}
	
	/**
	 * Same as createProject but allows setting a datapath
	 * @param client {@link MongoClient} for db access
	 * @param vertx {@link Vertx} instance for communication
	 * @param projectName name of the project
	 * @param datapath the datapath to use 
	 * @return  A {@link Future} of a {@link JsonObject} representing the Project
	 */
	public static Future<JsonObject> createProject(MongoClient client, Vertx vertx, String projectName, String datapath)
	{
		ElementManager<Project> projectManager = new ElementManager<>(Project::new, APIProject::new, client, vertx); 
		 ElementManager<Experiment> expManager = new ElementManager<>(Experiment::new, APIExperiment::new, client, vertx);
		 ElementManager<Task> taskManager = new ElementManager<>(Task::new, APITask::new, client, vertx);
		 return buildAPIProject(projectManager, expManager, taskManager, client, projectName, datapath).map(res -> { return new JsonObject().put("UUID", res.getUUID()).put("version", res.getVersion());});
	}
	
	/**
	 * Update the Element targets of the given experiments (i.e. the elements in it)
	 * @param experiment the experiment JSON to update
	 */
	private static void updateExperimentElementTargets(JsonObject experiment)
	{
		String experimentInstanceID = experiment.getString("instanceID");
		JsonArray elements = experiment.getJsonArray("elements", new JsonArray()); 
		for(int i = 0; i < elements.size(); i++)
		{
			JsonObject celement = elements.getJsonObject(i).getJsonObject("data");
			String etype = elements.getJsonObject(i).getString("elementType");
			LOGGER.debug(celement.encodePrettily());
			switch(etype)
			{
			case "task": if(celement.getString("next").equals("end"))
							{
							celement.put("next", experimentInstanceID);
							}
						 break;
			case "experiment":
						updateExperimentElementTargets(celement);
						break;
			case "filter": updateFilterTargets(celement, experimentInstanceID);
						   break;
				
			}
		}
	}
	/**
	 * Update the filter targets 
	 * @param filter the filter to update
	 * @param sourceID the source id to set as target.
	 */
	private static void updateFilterTargets(JsonObject filter, String sourceID)
	{
		if(filter.getString("defaultOption").equals("end"))
		{
			filter.put("defaultOption", sourceID);
		}
		JsonArray options = filter.getJsonArray("options");
		for(int i = 0 ; i < options.size(); i++)
		{
			JsonObject option = options.getJsonObject(i);
			if(option.getString("next").equals("end"))
			{
				option.put("next", sourceID);
			}
		}
	}
	
	/**
	 * Build a PI Task
	 * @param manager the {@link ElementManager} for tasks
	 * @param elementID the ID of the element
	 * @param datapath the datapath
	 * @return a {@link Future} of an {@link APITask}
	 */
	@SuppressWarnings("rawtypes")
	public static Future<APITask> buildAPITask(ElementManager<Task> manager, String elementID, String datapath)
	{
		Promise<APITask> taskPromise = Promise.promise();
		try
		{
			JsonObject TaskDef;			
			String TaskCode;
			String TestDataFolder;

			if(datapath == null)
			{
				TaskDef = new JsonObject(Files.readString(Paths.get(ObjectGenerator.class.getClassLoader().getResource("APITestData/TaskData.json").getPath()))).getJsonObject(elementID);			
				LOGGER.debug(elementID + ":");
				LOGGER.debug(TaskDef.encodePrettily());
				TaskCode = Files.readString(Paths.get(ObjectGenerator.class.getClassLoader().getResource("CodeTestData/" + TaskDef.getString("codeFile")).getPath()));
				TestDataFolder = ObjectGenerator.class.getClassLoader().getResource("FileTestData").getPath();
			}
			else
			{
				TaskDef = new JsonObject(Files.readString(Paths.get(datapath, "APITestData/TaskData.json"))).getJsonObject(elementID);			
				TaskCode = Files.readString(Paths.get(datapath, "CodeTestData", TaskDef.getString("codeFile")));
				TestDataFolder = Paths.get(datapath, "FileTestData").toString();				
			}
			String dataDir = Files.createTempDirectory("TaskDataFolder").toAbsolutePath().toString();
			FileUtils.copyDirectory(new File(TestDataFolder), new File(dataDir));
			manager.createOrLoadElement(TaskDef.getString("name"),TaskDef.getString("codeType"),TaskDef.getString("codeVersion"))
			.onSuccess(task -> {
				LOGGER.debug("Task object created");
				JsonArray resources = TaskDef.getJsonArray("resources", new JsonArray());
				Promise<String> versionPromise = Promise.promise();
				Future<String> versionFuture = versionPromise.future();
				List<Future<String>> composite = new LinkedList<>();
				LinkedList<Future<String>> chain = new LinkedList<>();
				chain.add(versionFuture);
				for(int i = 0; i < resources.size(); ++i)
				{
					SOILEUpload upload = SOILEUpload.create(Path.of(dataDir, resources.getString(i)).toString(), resources.getString(i), MimeMapping.mimeTypeForFilename(resources.getString(i)));
					// create all in a compose chain...
					String resourceName = resources.getString(i);
					chain.add(chain.getLast().compose(newVersion -> {
						LOGGER.debug("Adding File " + resourceName + " to Version " + newVersion);
						return manager.handlePostFile(task.getUUID(), newVersion, resourceName, upload);					
					}));
					composite.add(chain.getLast());
				}
				versionPromise.complete(task.getCurrentVersion());
				Future.all(composite)
				.onSuccess(done -> {
					LOGGER.debug("File(s) added");
					chain.getLast().onSuccess(latestVersion ->
					{
					APITask apiTask = new APITask(TaskDef);				
					apiTask.loadGitJson(TaskDef);
					apiTask.setCode(TaskCode);
					apiTask.setVersion(latestVersion);
					apiTask.setUUID(task.getUUID());						
					task.setPrivate(apiTask.getPrivate());									
						manager.updateElement(apiTask,"Initial_Version")
						.onSuccess(newVersion -> {
							LOGGER.debug("Api Task Updated");
							apiTask.setVersion(newVersion);
							try {
								FileUtils.deleteDirectory(new File(dataDir));
							}
							catch(IOException e)
							{
								
							}
							taskPromise.complete(apiTask);							
						})
						.onFailure(err -> taskPromise.fail(err));

					})
					.onFailure(err -> taskPromise.fail(err));
				})
				.onFailure(err -> taskPromise.fail(err));
				
			})
			.onFailure(fail -> taskPromise.fail(fail));
		}
		catch(IOException e)
		{
			taskPromise.fail(e);
		}
		return taskPromise.future();
	}
	
	
}
