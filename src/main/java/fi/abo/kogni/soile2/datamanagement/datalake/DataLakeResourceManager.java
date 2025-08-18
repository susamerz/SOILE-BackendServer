package fi.abo.kogni.soile2.datamanagement.datalake;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.git.GitDataRetriever;
import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.http_server.requestHandling.SOILEUpload;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.http.MimeMapping;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.FileUpload;

/**
 * A Manager/Retriever for Files for which indicator files are stored in the git Repository.
 * Each Repository that contains resources will be represented by one folder in the datalake which will contain all data.    
 * @author thomas
 *
 */
public class DataLakeResourceManager extends GitDataRetriever<DataLakeFile> {

	private static final Logger LOGGER = LogManager.getLogger(DataLakeResourceManager.class);

	private String dataLakeDirectory;
	private Vertx vertx;
	/**
	 * Default construtor getting the vertx instance
	 * @param vertx The vertx instance for communications
	 */
	public DataLakeResourceManager(Vertx vertx)
	{
		super(vertx.eventBus(),true, true);
		this.vertx = vertx;
		dataLakeDirectory = SoileConfigLoader.getServerProperty("soileGitDataLakeFolder");

	}

	/**
	 * Create/update a git file pointing to the actual element in the datalake that this file represents. This function will MOVE the
	 * file referenced by the provided {@link SOILEUpload}, so the {@link SOILEUpload} will not be valid afterwards.
	 * @param targetGitFile the {@link GitFile} the new data should be written to. Indicating the location in the resources
	 * @param is the {@link InputStream} that contains the data to be associated with the targetGitFile
	 * @return A {@link Future} of the new version written
	 */
	public Future<String> writeStreamToGit(GitFile targetGitFile, InputStream is)	
	{		
		Promise<String> versionUpdatePromise = Promise.promise();
		writeStreamToDataLake(targetGitFile, is)
		.onSuccess(dataFile -> {
			LOGGER.debug("File moved to target folder");
			String gitFileName = targetGitFile.getFileName();
			String targetFileName = dataFile.getFileInDataLake();		
			JsonObject fileContents = new JsonObject().put("filename", gitFileName)
					.put("targetFile", targetFileName)
					.put("format", MimeMapping.mimeTypeForFilename(gitFileName));
			eb.request("soile.git.writeGitResourceFile", targetGitFile.toJson().put("data", fileContents))
			.onSuccess(reply -> {
						LOGGER.debug("Git File written");
						versionUpdatePromise.complete((String) reply.body());
					})
			.onFailure(err -> 
			{
				deleteFile(dataFile)
				.onFailure(err2 -> {
					// TODO: Log the error in deleting this file.
				});
				versionUpdatePromise.fail(err);
			});
		})
		.onFailure(err -> versionUpdatePromise.fail(err));
		return versionUpdatePromise.future();

	}
	
	
	/**
	 * Create/update a git file pointing to the actual element in the datalake that this file represents. This function will MOVE the
	 * file referenced by the provided {@link SOILEUpload}, so the {@link SOILEUpload} will not be valid afterwards.
	 * @param targetGitFile the {@link GitFile} the new data should be written to. Indicating the location in the resources
	 * @param fileUpload the {@link FileUpload} that contains the data to be associated with the targetGitFile
	 * @return a {@link Future} of the version written
	 */
	public Future<String> writeUploadToGit(GitFile targetGitFile, SOILEUpload fileUpload)	
	{		
		Promise<String> versionUpdatePromise = Promise.promise();
		moveUploadToDataLake(targetGitFile, fileUpload)
		.onSuccess(dataFile -> {
			LOGGER.debug("File moved to target folder");
			String gitFileName = targetGitFile.getFileName() == null ? fileUpload.fileName() : targetGitFile.getFileName();
			String targetFileName = dataFile.getFileInDataLake();		
			String fileType = MimeMapping.mimeTypeForFilename(gitFileName) == null ? fileUpload.contentType() : MimeMapping.mimeTypeForFilename(gitFileName);    
			JsonObject fileContents = new JsonObject().put("filename", gitFileName)
					.put("targetFile", targetFileName)
					.put("format", fileType);
			eb.request("soile.git.writeGitResourceFile", targetGitFile.toJson().put("data", fileContents))
			.onSuccess(reply -> {
						LOGGER.debug("Git File written");
						versionUpdatePromise.complete((String) reply.body());
					})
			.onFailure(err -> 
			{
				deleteFile(dataFile)
				.onFailure(err2 -> {
					// TODO: Log the error in deleting this file.
					LOGGER.error(err2, err2);
				});
				versionUpdatePromise.fail(err);
			});
		})
		.onFailure(err -> versionUpdatePromise.fail(err));
		return versionUpdatePromise.future();

	}

	/**
	 * Delete a file from a version in git
	 * @param targetGitFile the {@link GitFile} the new data should be written to. Indicating the location in the resources
	 * @return a {@link Future} of the version written
	 */
	public Future<String> deleteGitFile(GitFile targetGitFile)	
	{		
		Promise<String> versionUpdatePromise = Promise.promise();
		eb.request("soile.git.deleteGitResourceFile", targetGitFile.toJson())
		.onSuccess(reply -> {
			LOGGER.debug("Git File written");
			versionUpdatePromise.complete((String) reply.body());
		})
		.onFailure(err ->versionUpdatePromise.fail(err));		
		return versionUpdatePromise.future();

	}
	
	/**
	 * Test, whether the git repository for a specific element (Task/Experiment/Project) exists
	 * @param elementID the UUID of the element
	 * @return A future whether the element exists
	 */
	public Future<Boolean> existElementRepo(String elementID)
	{
		return eb.request("soile.git.doesRepoExist",elementID).map(reply -> {return (Boolean) reply.body();});
	}

	@Override
	public DataLakeFile createElement(Object elementData, GitFile key) {

		JsonObject json = (JsonObject) elementData;
		GitDataLakeFile representedElement = new GitDataLakeFile(key.getRepoID(), json.getString("targetFile"), json.getString("filename"), json.getString("format"));								
		return representedElement.getDataLakeFile();
	}
	
	/**
	 * Move a File from a {@link FileUpload} to a a folder in the datalake that is associated with the repository indicated by the provided {@link GitFile} 
	 * @param targetGitFile the target {@link GitFile} the upload data will be associated with 
	 * @param upload the Upload from which to extract the actual file.
	 * @return a {@link Future} of the {@link GitDataLakeFile} that contains all information about the datalake file created. 
	 */
	public Future<GitDataLakeFile> moveUploadToDataLake(GitFile targetGitFile, SOILEUpload upload)
	{
		Promise<GitDataLakeFile> dataLakeFilePromise = Promise.promise();
		String targetFolder = Path.of(dataLakeDirectory, targetGitFile.getRepoID()).toAbsolutePath().toString();		
		vertx.fileSystem().mkdirs(targetFolder)
		.onSuccess(folderCreated -> {			
			vertx.fileSystem().createTempFile(targetFolder , "", "","rw-rw----")
			.onSuccess(tempFileName -> {	
				vertx.fileSystem().move(upload.uploadedFileName(), tempFileName, new CopyOptions().setReplaceExisting(true))
				.onSuccess(moved -> {
					GitDataLakeFile result = new GitDataLakeFile(targetGitFile.getRepoID(), tempFileName.replace(targetFolder, ""), upload.fileName(), upload.contentType());
					dataLakeFilePromise.complete(result);
				})
				.onFailure(err -> dataLakeFilePromise.fail(err));
			}).onFailure(err -> dataLakeFilePromise.fail(err));
		})
		.onFailure(err -> dataLakeFilePromise.fail(err));
		return dataLakeFilePromise.future();	
	}	

	/**
	 * Delete a Task folder and all its contents from the dataLake. 
	 * @param RepoID The ID of the Repository belonging to the task.
	 * @return A Future that succeeded if the path was deleted.
	 */
	public Future<Void> deleteElementFolder(String RepoID)
	{
		Promise<Void> dataLakeFolderRemovedPromise = Promise.promise();
		String targetFolder = Path.of(dataLakeDirectory, RepoID).toAbsolutePath().toString();
		vertx.fileSystem().exists(targetFolder)
		.compose(exists -> {
			if(exists)
			{
				return vertx.fileSystem().deleteRecursive(targetFolder);
			}
			else
			{
				// we still continue since this is fine, as the folder no longer exists.. 
				LOGGER.warn("Trying to delete non existing folder " + targetFolder);
				return Future.succeededFuture();
			}		
		})
		.onSuccess(folderRemoved -> {			
			dataLakeFolderRemovedPromise.complete();
		})		
		.onFailure(err -> dataLakeFolderRemovedPromise.fail(err));
		return dataLakeFolderRemovedPromise.future();
	}
	
	/**
	 * Write the data from an {@link OutputStream} to the datalake, creating a new file 
	 * @param targetGitFile the target {@link GitFile} the upload data will be associated with 
	 * @param is the {@link InputStream} containing the data for the actual file.
	 * @return a {@link Future} of the {@link GitDataLakeFile} that contains all information about the datalake file created. 
	 */
	public Future<GitDataLakeFile> writeStreamToDataLake(GitFile targetGitFile, InputStream is)
	{
		Promise<GitDataLakeFile> dataLakeFilePromise = Promise.promise();
		String targetFolder = Path.of(dataLakeDirectory, targetGitFile.getRepoID()).toAbsolutePath().toString();		
		vertx.fileSystem().mkdirs(targetFolder)
		.onSuccess(folderCreated -> {			
			vertx.fileSystem().createTempFile(targetFolder , "", "","rw-rw----")
			.onSuccess(tempFileName -> {
				vertx.executeBlocking( () -> {
					FileOutputStream outputStream = new FileOutputStream(tempFileName);
						{
							int data = is.read();
							while(data != -1){
								outputStream.write(data);
								data = is.read();
							}
						}	
						outputStream.close();
						return "";					
				}).onSuccess(done -> {
					GitDataLakeFile trf = new GitDataLakeFile(targetGitFile.getRepoID(), tempFileName.replace(targetFolder, ""), targetGitFile.getFileName(), MimeMapping.mimeTypeForFilename(targetGitFile.getFileName()));
					dataLakeFilePromise.complete(trf);
				})
				.onFailure(err -> dataLakeFilePromise.fail(err));

			})
			.onFailure(err -> dataLakeFilePromise.fail(err));		
		})
		.onFailure(err -> dataLakeFilePromise.fail(err));
		return dataLakeFilePromise.future();	
	}	

	/**
	 * Delete a file in the datalake associated with the given git file.
	 * Be careful the file will be deleted, so make sure it's not associated with something else.
	 * @param toDelete The file to delete
	 * @return A Future of the operation succeeding.
	 */
	public Future<Void> deleteFile(GitDataLakeFile toDelete)
	{
		return vertx.fileSystem().delete(toDelete.getDataLakeFile().getAbsolutePath());
	}
	
	/**
	 * Remove a File for a specific taskID in the data lake
	 * @param taskID the task for which to delete a file
	 * @param dataLakeFileName the name of the file in the data lake
	 * @return A Future of the operation succeeding.
	 */
	public Future<Void> deleteDataLakeFile(String taskID, String dataLakeFileName)
	{
		File toDelete = getTaskDataLakeFile(taskID, dataLakeFileName);		
		return vertx.fileSystem().delete(toDelete.getAbsolutePath());		
	}	
	
	/**
	 * Get the {@link File} specified by the taskID and the filename in the resource datalake.
	 * @param taskID - the task for which to retrieve the file
	 * @param dataLakeFileName the name of the datalake file.
	 * @return the Actual File object corresponding to the dataLakeFilename in the given task
	 */
	public File getTaskDataLakeFile(String taskID, String dataLakeFileName)
	{	
		return new File(dataLakeDirectory + File.separator + Task.typeID + taskID + File.separator + dataLakeFileName);
	}
	
}
