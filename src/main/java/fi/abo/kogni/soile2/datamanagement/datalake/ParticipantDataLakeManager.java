package fi.abo.kogni.soile2.datamanagement.datalake;

import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.projecthandling.participant.Participant;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.CopyOptions;
import io.vertx.ext.web.FileUpload;

/**
 * This class represents a DataLakeManager for data in projects. 
 * The Data is ordered as follows:
 * Project 
 * 	- participantID1
 *  	- {@literal <}taskID>
 *   		- {@literal <}step> (a number)
 *   			- localFileName
 *   		- {@literal <}step> (a number)
 *   			- localFileName
 *   	- {@literal <}taskID>
 *   		- {@literal <}step> (a number)
 *   			- localFileName
 *   			...
 *   - participantID2 
 *   	...
 * 
 * @author Thomas Pfau
 *
 */
public class ParticipantDataLakeManager{

	String datalakedirectory;
	Vertx vertx;
	static final Logger LOGGER = LogManager.getLogger(ParticipantDataLakeManager.class);
	
	/**
	 * Default constructor 
	 * @param Folder The datalake folder to use 
	 * @param vertx the vertx instance for communication
	 */
	public ParticipantDataLakeManager(String Folder, Vertx vertx) {
		datalakedirectory = Folder;
		this.vertx = vertx; 
	}

	/**
	 * Write an element to into the dataLake. The actual location in the dataLake is defined by the task ID and the participant ID.
	 * Data will be stored in the following scheme (to be able to quickly download all data for a project):  
	 * studyID / participantID / step / taskID / filesForTask.format    
	 * This allows a quick removal of all data for a participant by deleting the respective participantID folder.
	 * @param participantID the participant for which to store the data
	 * @param step the step for the data 
	 * @param taskID the task for which data is stored.
	 * @param upload The fileUpload containing the data
	 * @return A {@link Future} of the version used
	 */
	public Future<String> storeParticipantData(String participantID, int step, String taskID, FileUpload upload)
	{
		LOGGER.debug(datalakedirectory);
		Promise<String> idPromise = Promise.promise();
		LOGGER.debug("Trying to store data for: " + participantID + " / " + step + " / " + taskID );
		ParticipantFileResult targetFile = new ParticipantFileResult("", "", "", step, taskID, participantID);
		LOGGER.debug("Creating directories for file: " + targetFile.toString());		
		vertx.fileSystem().mkdirs(targetFile.getFolderPath(datalakedirectory))		
		.onSuccess( folderCreated -> {
			LOGGER.debug("Directories created, creating Temp File");
			vertx.fileSystem().createTempFile(targetFile.getFolderPath(datalakedirectory), "result", ".out","rw-rw----")
			.onSuccess(targetFileLocation -> {
				// This is an absolute fileName ! So we need to just take the actual last bit of it.
				LOGGER.debug("temp File created: " + targetFileLocation);
				LOGGER.debug("Folder is: " + targetFile.getFolderPath(datalakedirectory));				
				String fileName = Path.of(targetFileLocation).getFileName().toString();
				LOGGER.debug(fileName);
				targetFile.setLocalFileName(fileName);
				LOGGER.debug("Trying to move file : " + upload.uploadedFileName() + " to " + targetFile.getFilePath(datalakedirectory));
				vertx.fileSystem().move(upload.uploadedFileName(), targetFile.getFilePath(datalakedirectory), new CopyOptions().setReplaceExisting(true))
				.onSuccess(res -> {
					LOGGER.debug("File Moved ");
					idPromise.complete(fileName);
				})
				.onFailure(err -> idPromise.fail(err));
			})
			.onFailure(err -> idPromise.fail(err));		
			
		})
		.onFailure(err -> idPromise.fail(err));
		
		return idPromise.future();	
	}

	/**
	 * Delete a participant from a study. This will remove the participant data from the project.
	 * @param p the participant for which to store the data
	 * @return A {@link Future} indicating whether the deletion was successful (if it succeeded)
	 */
	public Future<Void> deleteParticipantData(Participant p)
	{
		ParticipantFileResults resultFolder = new ParticipantFileResults(p.getID()); 
		return vertx.fileSystem().deleteRecursive(resultFolder.getParticipantFolderPath(datalakedirectory));
	}
	/**
	 * Get a File from a TaskFileResult
	 * @param result the result for which to obtain the file location in the datalake
	 * @return The corresponding {@link DataLakeFile}
	 */
	public DataLakeFile getFile(ParticipantFileResult result)
	{
		LOGGER.debug("Requesting file for directory" + datalakedirectory);
		LOGGER.debug(result.toString());
		return result.getFile(datalakedirectory);		
	}
}
