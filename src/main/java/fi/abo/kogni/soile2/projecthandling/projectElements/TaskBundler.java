package fi.abo.kogni.soile2.projecthandling.projectElements;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.aalto.scicomp.zipper.FileDescriptor;
import fi.aalto.scicomp.zipper.FileDescriptorImpl;
import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeResourceManager;
import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.projecthandling.apielements.APITask;
import fi.abo.kogni.soile2.projecthandling.exceptions.FileContentInvalidException;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
/**
 * A Class that facilitates creating a zip bundle for a given task.
 * @author Thomas Pfau
 *
 */
public class TaskBundler {
	private static final Logger LOGGER = LogManager.getLogger(TaskBundler.class);

	DataLakeResourceManager dlrmgr;
	EventBus eb;
	Vertx vertx;
	ElementManager<Task> taskManager;
	/**
	 * SOURCE File name
	 */
	public static String SOURCEFILE_NAME = "source.code";
	/**
	 * Configuration file name
	 */
	public static String CONFIGFILE_NAME = "soile.cfg";
	/**
	 * Resource folder name
	 */
	public static String RESOURCEFOLDER_NAME = "resources";
	
		
	/**
	 * Default constructor
	 * @param dlrmgr Manager for access to the data lake
	 * @param eb {@link EventBus} for communication 
	 * @param vertx {@link Vertx} instance for communicaton
	 * @param taskManager TaskManager to access task data
	 */
	public TaskBundler(DataLakeResourceManager dlrmgr, EventBus eb, Vertx vertx, ElementManager<Task> taskManager) {
		super();
		this.dlrmgr = dlrmgr;
		this.eb = eb;
		this.vertx = vertx;
		this.taskManager = taskManager;
	}

	/**
	 * Retrieve a list of the files linked to this version. This returns {@link FileDescriptor}s which point to the actual files in the dataLake.
	 * @param taskUUID
	 * @param version
	 * @return
	 */
	private Future< List<FileDescriptor> > extractResourceFileList(String taskUUID, String version)
	{
		Promise<List<FileDescriptor>> listPromise = Promise.promise();
		eb.request("soile.git.getResourceList", new JsonObject().put("repoID", taskManager.getGitIDForUUID(taskUUID)).put("version", version))
		.onSuccess(resourceList -> {			
			JsonArray resourceArray = (JsonArray)resourceList.body();
			// this has the structure: [ { label : file/foldername , children: [ {} ] <optional> }];
			List<Future<Void>> fileRetrievalFutures = new LinkedList<>();
			ConcurrentLinkedDeque<FileDescriptor> resources = new ConcurrentLinkedDeque<FileDescriptor>();
			buildResourceList(resourceArray, fileRetrievalFutures, resources, Path.of(""), taskUUID, version);
			Future.all(fileRetrievalFutures)
			.onSuccess(allFilesRetrieved -> {
				// now we got all The resource files now. 
				listPromise.complete(List.copyOf(resources));
			})
			.onFailure(err -> listPromise.fail(err));
		})
		.onFailure(err -> listPromise.fail(err));		
		
		return listPromise.future();
	}
	
	private void buildResourceList(JsonArray fileList, List<Future<Void>> fileRetrievalFutures, ConcurrentLinkedDeque<FileDescriptor> files, Path currentPath, String taskUUID, String version)
	{
		for(int i = 0; i < fileList.size(); ++i)
		{
			JsonObject currentFile = fileList.getJsonObject(i);
			if(currentFile.containsKey("children"))
			{
				buildResourceList(currentFile.getJsonArray("children"), fileRetrievalFutures, files, currentPath.resolve(currentFile.getString("label")), taskUUID, version);
			}
			else
			{
				Promise<Void> fileRetrivalPromise = Promise.promise();
				fileRetrievalFutures.add(fileRetrivalPromise.future());
				GitFile target = new GitFile(currentPath.resolve(currentFile.getString("label")).toString(), taskManager.getGitIDForUUID(taskUUID), version); 
				dlrmgr.getElement(target)
				.onSuccess(datalakeFile -> {
					// this should go to the resource folder.
					datalakeFile.setOriginalFileName(currentPath.resolve(currentFile.getString("label")).toString());
					files.add(datalakeFile);
					fileRetrivalPromise.complete();
				})
				.onFailure(err -> fileRetrivalPromise.fail(err));
			}
		}	
	}
	/**
	 * Create a Temporary file with the source code of the 
	 */
	private Future< List<FileDescriptor> > createCodeAndConfigFiles(String taskUUID, String version)
	{
		Promise<List<FileDescriptor>> listPromise = Promise.promise();
		eb.request("soile.task.getAPIData", new JsonObject().put("UUID", taskUUID).put("version", version))
		.onSuccess(apiData -> {
			JsonObject taskData = ((JsonObject)apiData.body()).getJsonObject(SoileCommUtils.DATAFIELD);
			String code = taskData.getString("code");
			// now, we need to remove a couple of entries.			
			taskData.remove("code");
			taskData.remove("version");
			taskData.remove("UUID");
			createTempFileAndWrite(taskUUID + version, ".code", code)
			.onSuccess(codeFile -> {
				createTempFileAndWrite(taskUUID + version, ".cfg", taskData.encodePrettily())
				.onSuccess(configFile -> {
					List<FileDescriptor> fileList = new LinkedList<>();
					fileList.add(new FileDescriptorImpl(codeFile, SOURCEFILE_NAME));
					fileList.add(new FileDescriptorImpl(configFile, CONFIGFILE_NAME));
					listPromise.complete(fileList);
				})
				.onFailure(err -> listPromise.fail(err));
			})
			.onFailure(err -> listPromise.fail(err));
		})
		.onFailure(err -> listPromise.fail(err));				
		return listPromise.future();
		
	}

	private Future<String> createTempFileAndWrite(String Prefix, String Suffix, String content)
	{
		return vertx.fileSystem().createTempFile(Prefix, Suffix).map(fileName -> { vertx.fileSystem().writeFile(fileName, Buffer.buffer(content));
																				   return fileName; } );
		
	}
	
	
	/**
	 * Create Code and config files and retrieve the {@link FileDescriptor}s   
	 * @param taskUUID - the given task
	 * @param version the version of the task
	 * @return a {@link Future} of a List of {@link FileDescriptor}s
	 */
	public Future<List<FileDescriptor>> buildTaskFileList(String taskUUID, String version)
	{
		Promise<List<FileDescriptor>> listPromise = Promise.promise();
		List<FileDescriptor> fileList = new LinkedList<>();
		createCodeAndConfigFiles(taskUUID, version)
		.onSuccess(codeAndConfig -> {
			fileList.addAll(codeAndConfig);
			extractResourceFileList(taskUUID, version)
			.onSuccess(resourceFiles -> {
				fileList.addAll(resourceFiles);
				listPromise.complete(fileList);
			})
			.onFailure(err -> listPromise.fail(version));
		})
		.onFailure(err -> listPromise.fail(version));
		return listPromise.future();
	}
	
	
	
	/**
	 * Same as {@link #createTaskFromFile(File, String)} but without providing a name, trying to use the name from the zip.
	 * @param TaskZipFile The File containing the zipped task
	 * @param Tag the Name of the Version given to this task.
	 * @return A {@link Future} that is successful if the task was created.  
	 */
	public Future<Task> createTaskFromFile(File TaskZipFile, String Tag)
	{
		return createTaskFromFile(TaskZipFile, Tag, null);
	}
	
	/**
	 * Create A Task using a given TaskName and the given File pointing to a Zip File containig the task information
	 * @param TaskZipFile  The File containing the zipped task
	 * @param Tag The tag of the task to retrieve
	 * @param taskName The name of the task (can be null)
	 * @return A {@link Future} that is successful if the task was created.
	 */
	public Future<Task> createTaskFromFile(File TaskZipFile, String Tag, String taskName)
	{		
		WorkerExecutor we = vertx.createSharedWorkerExecutor("unzip",6,2,TimeUnit.MINUTES);
		Promise<Task> taskCreatedPromise = Promise.promise(); 
		// First, we need to fetch the code and the config
		boolean codeFound = false;
		boolean configFound = false;
		String code = null;
		JsonObject config = null;			
		try(ZipFile zipFile = new ZipFile(TaskZipFile)) {

			Enumeration<? extends ZipEntry> entries = zipFile.entries();

			while(entries.hasMoreElements() && ( !codeFound || !configFound)){
				ZipEntry entry = entries.nextElement();
				String entryName = entry.getName();

				if(entryName.equals(SOURCEFILE_NAME))
				{

					InputStream is = zipFile.getInputStream(entry);
					try {
						code = IOUtils.toString(is, StandardCharsets.UTF_8);
						codeFound = true;
					}
					finally
					{
						IOUtils.closeQuietly(is);
					}
				}
				if(entryName.equals(CONFIGFILE_NAME))
				{
					InputStream is = zipFile.getInputStream(entry);
					try {
						String conf = IOUtils.toString(is, StandardCharsets.UTF_8);
						config = new JsonObject(conf);
						if(taskName != null)
						{
							config.put("name", taskName);
						}
						configFound = true;
					}
					finally
					{
						IOUtils.closeQuietly(is);
					}
				}
			}				
		}
		catch(IOException e)
		{
			taskCreatedPromise.fail(e);		
			return taskCreatedPromise.future();
		}	
		
		if(code != null && config != null)
		{
			String usedCode = code;
			JsonObject usedConfig = config;
			taskManager.createElement(config.getString("name"), config.getJsonObject("codeType").getString("language"), config.getJsonObject("codeType").getString("version"))
			.onSuccess(createdTask -> {				
				try {
					ZipFile zipFile = new ZipFile(TaskZipFile);
					Promise<String> currentVersionPromise = Promise.promise();
					Future<String> currentVersionFuture = currentVersionPromise.future();
					Enumeration<? extends ZipEntry> entries = zipFile.entries();
					List<Future<String>> FilesExtractedList = new LinkedList<>();						
					LinkedList<Future<String>> versionsList = new LinkedList<>();
					versionsList.add(currentVersionFuture);
					while(entries.hasMoreElements()){
						ZipEntry entry = entries.nextElement();
						if (!entry.getName().equals(SOURCEFILE_NAME) && !entry.getName().equals(CONFIGFILE_NAME) && !entry.isDirectory())
						{
							String filePath = entry.getName();															
							InputStream zipIn = zipFile.getInputStream(entry);
							// this ensures the correct order.
							versionsList.add(versionsList.getLast().compose(version -> {								
								return taskManager.handleWriteFileToObject(createdTask.getUUID(),version,filePath,zipIn);								
								}));
							
							FilesExtractedList.add(versionsList.getLast());
						}											
						// now, we want to skip "resources folder"
					}
					currentVersionPromise.complete(createdTask.getCurrentVersion());
					Future.all(FilesExtractedList)
					.onSuccess(success -> {
						versionsList.getLast().onSuccess(latestVersion -> {
							taskManager.getAPIElementFromDB(createdTask.getUUID(), latestVersion)
							.onSuccess(apiTask -> {
								APITask update = new APITask(apiTask.getAPIJson().mergeIn(usedConfig));								
								update.setCode(usedCode);
								update.setVersion(latestVersion);
								taskManager.updateElement(update,Tag)
								.onSuccess(newVersion -> {	
									createdTask.setCurrentVersion(newVersion);
									try
									{
										zipFile.close();
									}
									catch(IOException e)
									{
										taskCreatedPromise.fail(e);
										return;
									}
									taskCreatedPromise.complete(createdTask);							
								})
								.onFailure(err -> taskCreatedPromise.fail(err));
							})
							.onFailure(err -> taskCreatedPromise.fail(err));
							
						})
						.onFailure(err -> taskCreatedPromise.fail(err));
					})
					.onFailure(err -> taskCreatedPromise.fail(err));
					
				}
				catch(IOException e)
				{
					taskCreatedPromise.fail(e);		
				}	

			})
			.onFailure(err -> {
				taskCreatedPromise.fail(err);					
			});
		}
		else
		{
			taskCreatedPromise.fail(new FileContentInvalidException());		
			return taskCreatedPromise.future();
		}

		return taskCreatedPromise.future();
	}	
	
	
}
