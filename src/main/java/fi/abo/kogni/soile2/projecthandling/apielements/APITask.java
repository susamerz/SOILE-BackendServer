package fi.abo.kogni.soile2.projecthandling.apielements;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.projecthandling.exceptions.NoCodeTypeChangeException;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Task;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;

/**
 * An API Task element
 * @author Thomas Pfau
 *
 */
public class APITask extends APIElementBase<Task> {
	/**
	 * Logger for this class
	 */
	private static final Logger LOGGER = LogManager.getLogger(APITask.class);

	private String[] gitFields = new String[] {"name", "codeType"};
	private Object[] gitDefaults = new Object[] {"", new JsonObject()};
	
	
	
	/**
	 * Default empty constructor
	 */
	public APITask() {
		this(new JsonObject());
	}
	
	/**
	 * Default constructor from Json Data
	 * @param data the data describing the Task
	 */
	public APITask(JsonObject data) {
		super(data);
		loadGitJson(data);
	}
	/**
	 * Get the type of the code (i.e. what language the code is in, to be able to 
	 * determine how it is going to be interpreted.
	 * This includes the Code type version (if that makes a difference).
	 * @return a JsonObject describing the code type (contaiing codeType and Version)
	 */
	public JsonObject getCodeType() {
		return data.getJsonObject("codeType", new JsonObject());
	}
	
	/**
	 * Set the type of the code (i.e. what language the code is in, to be able to 
	 * determine how it is going to be interpreted.
	 * @param codeType The JsonObject containing codeType(language) and codeVersion (language version)  
	 */
	public void setCodetype(JsonObject codeType) {
		data.put("codeType", codeType);
	}
	
	/**
	 * Get the code type version (i.e. the version of the language the code is in.
	 * Might make a difference for some code 
	 * @return the code version as a string e.g. 2.1.3
	 */
	public String getCodeVersion() {
		return getCodeType().getString("version","");
	}
	/**
	 * Set the code type version
	 * @param codeVersion the version of the language used for the code
	 */
	public void setCodeVersion(String codeVersion) {		
		JsonObject codeType = data.getJsonObject("codeType");
		if(codeType != null)
		{
			codeType.put("version", codeVersion);
		}
		else
		{
			data.put("codeType", new JsonObject().put("version", codeVersion));
		}
	}
	/**
	 * Get the language the code was written in.
	 * @return the language the code was written in
	 */
	public String getCodeLanguage() {
		return getCodeType().getString("language", "");
	}
	
	/**
	 * Set the language the code is written in.
	 * @param language the language the code is written in
	 */
	public void setCodeLanguage(String language) {
		JsonObject codeType = data.getJsonObject("codeType");
		if(codeType != null)
		{
			codeType.put("language", language);
		}
		else
		{
			data.put("codeType", new JsonObject().put("language", language));
		}
	}
	/**
	 * Get the code of the Task
	 * @return the code of the Task
	 */
	public String getCode() {
		return data.getString("code","");
	}
	/**
	 * Set the code of the task
	 * @param code the code of the task
	 */
	public void setCode(String code) {
		data.put("code", code);
	}
	
	@Override
	public void setElementProperties(Task task) throws NoCodeTypeChangeException
	{
		task.setAuthor(this.data.getString("author", task.getAuthor()));
		task.setDescription(this.data.getString("description", task.getDescription()));
		task.setKeywords(this.data.getJsonArray("keywords", task.getKeywords()));
		task.setLanguage(this.data.getString("language", task.getLanguage()));
		task.setType(this.data.getString("type", task.getType()));
	}
	@Override
	public JsonObject getGitJson() {
		
		JsonObject gitData = new JsonObject();
		for(int i = 0; i < gitFields.length ; ++i)
		{
			gitData.put(gitFields[i], data.getValue(gitFields[i], gitDefaults[i]));	
		}
		return gitData;
	}
	@Override
	public void loadGitJson(JsonObject json) {
		LOGGER.debug("Data before addition: " + data.encodePrettily() );
		for(int i = 0; i < gitFields.length ; ++i)
		{
			this.data.put(gitFields[i], json.getValue(gitFields[i], gitDefaults[i]));	
		}
		LOGGER.debug("Data after addition: " + data.encodePrettily() );
	}
	
	@Override
	public boolean hasAdditionalGitContent()
	{
		return true;
	}
	@Override
	public Future<String> storeAdditionalData(String currentVersion, EventBus eb, String targetRepository)
	{
		// We need to store the code. Resources are stored individually.
		
		GitFile g = new GitFile("Code.obj", targetRepository, currentVersion);
		LOGGER.debug("Writing Source Code");
		return eb.request("soile.git.writeGitFile", g.toJson().put("data",getCode())).map(message -> {return (String) message.body();});
	}
	@Override
	public Future<Boolean> loadAdditionalData(EventBus eb, String targetRepository)
	{
		Promise<Boolean> codePromise = Promise.promise();
		GitFile g = new GitFile("Code.obj", targetRepository, this.getVersion());
		LOGGER.debug("Loading Code Object");
		List<Future<Void>> loadedList = new LinkedList<>();
		loadedList.add(codePromise.future().mapEmpty());
		eb.request("soile.git.getGitFileContents", g.toJson()).onSuccess(codeReply -> {
			setCode((String)codeReply.body());
			codePromise.complete(true);
		})
		.onFailure(err -> {
			if(err instanceof ReplyException)
			{
				int errorCode = ((ReplyException)err).failureCode();
				if(errorCode == 404) // not found
				{
					// this is fine, could be that it wasn't generated yet, we will return an empty string then.
					setCode("");
					codePromise.complete(true);
				}
				else
				{
					codePromise.fail(err);		
				}
			}
			else
			{
				LOGGER.error("Failed to load Code File", err);				
				codePromise.fail(err);
			}
		});			
		return Future.all(loadedList).map(true);
	}	
	@Override
	public Function<Object,Object> getFieldFilter(String fieldName)
	{
		if(fieldName.equals("created"))
		{
			// we do not allow updating the created timestamp. 
			return (x) -> this.data.getValue(fieldName);
		}
		// all Fields in a task can be directly mapped. NO change needed.
		return (x) -> {return x;};
	}
	
	@Override
	public JsonObject calcDependencies() {
		return new JsonObject();		
	}
	
	@Override
	public void loadFromDBElement(Task element) {
		super.loadFromDBElement(element);
		this.data.put("author", element.getAuthor());
		this.data.put("description", element.getDescription());
		this.data.put("keywords", element.getKeywords());
		this.data.put("language", element.getLanguage());
		this.data.put("type", element.getType());
		this.data.put("created", element.getCreated());
	}
}
