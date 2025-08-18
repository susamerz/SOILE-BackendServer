package fi.abo.kogni.soile2.http_server;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.util.UriEncoder;

import fi.aalto.scicomp.zipper.FileDescriptor;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import fi.abo.kogni.soile2.utils.WebObjectCreator;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.MimeMapping;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientSession;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.multipart.MultipartForm;

/**
 * This class is a base class for Web Tests. It provides several convenience functions for use within tests. 
 * @author Thomas Pfau
 *
 */
public abstract class SoileWebTest extends SoileVerticleTest implements UserVerticleTest{

	protected WebClientSession generatorSession;
	private static final Logger LOGGER = LogManager.getLogger(SoileWebTest.class);

	/**
	 * Create a Master Token for the given project using the given client 
	 * @param client
	 * @param projectID
	 * @return
	 */
	protected Future<String> createMasterToken(WebClient client, String projectID)
	{
		return createTokens(client,projectID,0,true).map(output -> {return output.getString(0);});
	}
	
	/**
	 * Create a Token and signup as user using the provided session.
	 * @param authedSession
	 * @param projectID
	 * @return
	 */
	protected Future<String> createTokenAndSignupUser(WebClient authedSession, String projectID)
	{
		return createTokens(authedSession, projectID,1,false)
				.compose(tokenArray -> {
					String token = tokenArray.getString(0);
					return signUpToProjectWithToken(createSession(), token, projectID);
				});		
	}

	/**
	 * Create andstart the Testproject it will get a shortCut "newShortCut"
	 * @param priv whether the instance should be private
	 * @return
	 */
	protected Future<String> createAndStartTestProject(boolean priv)
	{
		return createAndStartTestProject(priv, "newShortCut");
	}
	
	/**
	 * Create andstart the Testproject with the given privacy setting and shortcut
	 * @param priv
	 * @param shortcut
	 * @return
	 */
	protected Future<String> createAndStartTestProject(boolean priv, String shortcut)
	{
		return createAndStartStudy(priv, shortcut, "Testproject");
	}
			
	
	protected Future<String> createAndStartStudy(WebClientSession session, boolean priv, String shortcut, String ProjectName)
	{
		JsonObject projectExec = new JsonObject().put("private", priv).put("name", "New Project").put("shortcut",shortcut); 
		Promise<String> projectInstancePromise = Promise.promise();
		if(session == null)
		{		
		createUserAndAuthedSession("Researcher", "test", Roles.Researcher)
		.onSuccess(authedSession -> {
			generatorSession = authedSession;
			WebObjectCreator.createProject(authedSession,ProjectName)
			.onSuccess(projectData -> {				
				String projectID = projectData.getString("UUID");
				String projectVersion = projectData.getString("version");
				POST(authedSession, "/project/" + projectID + "/" + projectVersion + "/init", null,projectExec )
				.onSuccess(response -> {
					POST(authedSession, "/study/" + response.bodyAsJsonObject().getString("projectID") + "/start", null, null )
					.onSuccess(activated -> {
						projectInstancePromise.complete(response.bodyAsJsonObject().getString("projectID"));	
					})
					.onFailure(err -> projectInstancePromise.fail(err));
					
					
				})
				.onFailure(err -> projectInstancePromise.fail(err));

			})
			.onFailure(err -> projectInstancePromise.fail(err));
		})
		.onFailure(err -> projectInstancePromise.fail(err));
		}
		else
		{
			WebObjectCreator.createProject(session, ProjectName)
			.onSuccess(projectData -> {				
				String projectID = projectData.getString("UUID");
				String projectVersion = projectData.getString("version");
				POST(session, "/project/" + projectID + "/" + projectVersion + "/init", null,projectExec )
				.onSuccess(response -> {
					POST(session, "/study/" + response.bodyAsJsonObject().getString("projectID") + "/start", null, null )
					.onSuccess(activated -> {
						projectInstancePromise.complete(response.bodyAsJsonObject().getString("projectID"));	
					})
					.onFailure(err -> projectInstancePromise.fail(err));
				})
				.onFailure(err -> projectInstancePromise.fail(err));

			})
			.onFailure(err -> projectInstancePromise.fail(err));
		}
		return projectInstancePromise.future();
	}
	
	/**
	 * Create and start the a project from the APIProjects with the given name 
	 * @param priv
	 * @param shortcut
	 * @param ProjectName
	 * @return
	 */
	protected Future<String> createAndStartStudy(boolean priv, String shortcut, String ProjectName)
	{
		return createAndStartStudy(generatorSession, priv, shortcut, ProjectName);
	}

	/**
	 * Check, whether the task is correct for the user authenticated in the given client session.
	 * @param client
	 * @param instanceID
	 * @param taskID
	 * @return
	 */
	protected Future<Void> checkTaskIsCorrect(WebClientSession client, String instanceID, String taskID)
	{
		Promise<Void> correctTask = Promise.promise();
		POST(client, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
		.onSuccess(nexttaskID -> {
			if(nexttaskID.bodyAsJsonObject().getString("id").equals(taskID))
			{
				correctTask.complete();				
			}
			else
			{
				correctTask.fail("Got " + nexttaskID.bodyAsString() + " expected " + taskID);
			}
		})
		.onFailure(err -> correctTask.fail(err));


		return correctTask.future();
	}
	
	/**
	 * Signup the session with the given token for the given project.
	 * @param client
	 * @param Token
	 * @param projectID
	 * @return
	 */
	protected Future<String> signUpToProjectWithToken(WebClientSession client,String Token, String projectID)
	{
		Promise<String> tokenPromise = Promise.promise();
		POST(client,"/study/" + projectID + "/signup", new JsonObject().put("token", Token), null)
		.onSuccess(response -> {
			tokenPromise.complete(response.bodyAsJsonObject().getString("token"));
		})
		.onFailure(err -> tokenPromise.fail(err));
		return tokenPromise.future();
	}

	/**
	 * Sign up to the given project. This requires the session to be allowed to sign up or it will fail.
	 * @param client
	 * @param projectID
	 * @return
	 */
	protected Future<Void> signUpToProject(WebClient client, String projectID)
	{
		Promise<Void> tokenPromise = Promise.promise();
		POST(client,"/study/" + projectID + "/signup", null, null)
		.onSuccess(response -> {
			tokenPromise.complete();
		})
		.onFailure(err -> tokenPromise.fail(err));
		return tokenPromise.future();
	}

	/**
	 * Create Tokens for the given project. If unique is false, the number of tokens will be equal to the provided count, 
	 * if true, only one master token will be created. 
	 * @param client
	 * @param projectID
	 * @param count
	 * @param unique
	 * @return a {@link JsonArray} {@link Future}. In case of unique being true, the array contains exactly one element which is the master token.
	 */
	protected Future<JsonArray> createTokens(WebClient client, String projectID, int count, boolean unique)
	{
		Promise<JsonArray> resultPromise = Promise.promise();
		POST(client,"/study/" + projectID + "/createtokens", new JsonObject().put("unique", unique).put("count", count), null )
		.onSuccess(response -> {
			if(unique)
			{
				resultPromise.complete(new JsonArray().add(response.bodyAsString()));
			}
			else
			{
				resultPromise.complete(response.bodyAsJsonArray());
			}
		})
		.onFailure(err -> resultPromise.fail(err));

		return resultPromise.future();
	}

	public static Future<HttpResponse<Buffer>> GET(WebClient client, String targetURL, JsonObject queryParameters, Object queryBody)
	{
		String host = SoileConfigLoader.getServerProperty("domain");
		int port = SoileConfigLoader.getServerIntProperty("port");
		HttpRequest<Buffer> request = client.get(port, host, UriEncoder.encode(targetURL));
		if(queryParameters != null)
		{
			for(String fieldname : queryParameters.fieldNames())
			{
				request.addQueryParam(fieldname, queryParameters.getString(fieldname));
			}
		}
		if(queryBody == null)
		{
			return request.send().compose(SoileWebTest::handleError);
		}
		else
		{

			if(queryBody instanceof JsonObject)
			{
				return request.sendJson((JsonObject)queryBody).compose(SoileWebTest::handleError);
					
			}
			else if(queryBody instanceof MultiMap)
			{
				return request.sendForm((MultiMap)queryBody).compose(SoileWebTest::handleError);
			}
			else
			{
				return request.sendBuffer(Buffer.buffer().appendString(queryBody.toString())).compose(SoileWebTest::handleError);
			}

		}
	}

	public static Future<JsonObject>  retrieveElementByName(WebClientSession webClient, String elementName, String elementType)
	{
		Promise<JsonObject> infoPromise = Promise.promise();
		// now, we need to get the Versions for the element and take the latest version.
		SoileWebTest.getElementList(webClient, elementType)
		.onSuccess(elementList -> {
			String UUID = null;
			for(int i = 0; i < elementList.size(); ++i)
			{
				if(elementList.getJsonObject(i).getString("name").equals(elementName))
				{
					UUID = elementList.getJsonObject(i).getString("UUID");
					break;
				}
			}
			if(UUID == null)
			{
				infoPromise.fail("Object supposedly exists but could not find an element in the list, possibly no access to the element");								
				return;
			}
			else
			{
				String elementID = UUID;
				SoileWebTest.getElementVersions(webClient, elementType, elementID)
				.onSuccess( res -> {
					// find the latest version.
					long newest = 0;
					String newestVersion = "";
					for(int i = 0; i < res.size(); i++)
					{
						if(res.getJsonObject(i).getLong("date") > newest)
						{
							newest = res.getJsonObject(i).getLong("date");
							newestVersion = res.getJsonObject(i).getString("version");
						}
					}
					if(newestVersion.equals(""))
					{
						infoPromise.fail("Couldn't find a version");
					}
					SoileWebTest.getElement(webClient, elementType, elementID, newestVersion)
					.onSuccess(latestJson -> {
						infoPromise.complete(latestJson);	
					})
					.onFailure(err2 -> infoPromise.fail(err2));									
				})
				.onFailure(err2 -> infoPromise.fail(err2));
			}

		})
		.onFailure(err2 -> infoPromise.fail(err2));

		return infoPromise.future();
	}

	public static Future<HttpResponse<Buffer>> POST(WebClient client, String targetURL, JsonObject queryParameters, Object queryBody)
	{
		String host = SoileConfigLoader.getServerProperty("domain");
		int port = SoileConfigLoader.getServerIntProperty("port");
		HttpRequest<Buffer> request = client.post(port, host, targetURL);
		if(queryParameters != null)
		{
			for(String fieldname : queryParameters.fieldNames())
			{
				request.addQueryParam(fieldname, queryParameters.getString(fieldname));
			}
		}
		if(queryBody == null)
		{
			return request.send().compose(SoileWebTest::handleError);
		}
		else
		{
			if(queryBody instanceof JsonObject)
			{
				return request.sendJsonObject((JsonObject)queryBody).compose(SoileWebTest::handleError);
			}
			else if(queryBody instanceof MultiMap)
			{
				return request.sendForm((MultiMap)queryBody).compose(SoileWebTest::handleError);
			}
			else
			{
				return request.sendBuffer(Buffer.buffer().appendString(queryBody.toString())).compose(SoileWebTest::handleError);
			}

		}
	}

	/**
	 * Handle a erronous response;
	 * @param response
	 * @return
	 */
	public static Future<HttpResponse<Buffer>> handleError(HttpResponse<Buffer> response)
	{
		if(response.statusCode() >= 400)
		{
			return Future.failedFuture(new HttpException(response.statusCode(),response.statusMessage()));
		}
		else
		{
			return Future.succeededFuture(response);
		}
	}

	public static Future<JsonObject> createNewElement(WebClient webClient, String elementAPI, JsonObject queryParameters)
	{
		return POST(webClient, "/" + elementAPI + "/create", queryParameters, null).map(res -> { return res.bodyAsJsonObject(); });				
	}

	public static Future<JsonObject> getElement(WebClient webClient, String elementAPI, String elementID, String Version)
	{
		return GET(webClient, "/" + elementAPI + "/" + elementID + "/" + Version + "/get", null, null).map(res -> {return res.bodyAsJsonObject(); });				
	}

	public static Future<JsonArray> getElementList(WebClient webClient, String elementAPI)
	{
		return POST(webClient, "/" + elementAPI + "/list", null, null).map(res -> { return res.bodyAsJsonArray(); });				
	}

	public static Future<JsonArray> getElementVersions(WebClient webClient, String elementAPI, String elementID)
	{
		return POST(webClient, "/" + elementAPI + "/" + elementID + "/list", null, null).map(res -> { return res.bodyAsJsonArray(); });				
	}


	/** Get the new version after posting a file to update the specified filename 
	 * 
	 * @param webClient
	 * @param TaskID
	 * @param TaskVersion
	 * @param Filename
	 * @param target
	 * @return
	 */
	public static Future<String> postTaskRessource(WebClient webClient, String TaskID, String TaskVersion, String Filename, File target, String mimeType)
	{
		String URL = "/task/" + TaskID + "/" + TaskVersion  + "/resource/" + Filename;
		return upload(webClient, URL, Filename, target, mimeType, "version");		
	}		
	
	/** Upload a file for a executing project and receive the File ID for results. 
	 * 
	 * @param webClient
	 * @param instanceID
	 * @param target
	 * @param Filename
	 * @param mimeType
	 * @return
	 */
	public static Future<String> uploadResult(WebClient webClient, String instanceID, File target, String Filename, String mimeType)
	{
		String URL = "/study/" + instanceID + "/uploaddata";
		return upload(webClient, URL, Filename, target, mimeType, "id");		
	}	
	/**
	 * Upload without query parameters
	 * @param client Client to use, 
	 * @param URL target URL
	 * @param fileName filename of the uploaded file
	 * @param uploadFile the uploaded File
	 * @param mimeType mimetype of the file
	 * @param idField 
	 * @return
	 */
	public static Future<String> upload(WebClient client, String URL, String fileName, File uploadFile, String mimeType, String idField)
	{			
		Promise<String> idPromise = Promise.promise();
		upload(client, URL, null, fileName, uploadFile, mimeType)
		.onSuccess(response -> {
			try {
				LOGGER.debug("Completing promise with idField:  " + idField);			
				idPromise.complete(response.getString(idField));
				LOGGER.debug("File uploaded");
			}
			catch(Exception e)
			{				
				idPromise.fail(e);
			}
		})
		.onFailure(err -> idPromise.fail(err));		
		return idPromise.future();
	}
	
	public static Future<JsonObject> upload(WebClient client, String URL, JsonObject queryParameters,
											String fileName, File uploadFile, String mimeType)
	{
		 
		HttpRequest<Buffer> request = client.post(UriEncoder.encode(URL));
		String currentMime = mimeType == null ? MimeMapping.mimeTypeForFilename(fileName) : mimeType;  
		if(queryParameters != null)
		{
			for(String fieldname : queryParameters.fieldNames())
			{
				request.addQueryParam(fieldname, queryParameters.getString(fieldname));
			}
		}
		MultipartForm submissionForm = MultipartForm.create()
				.binaryFileUpload(fileName, fileName, uploadFile.getAbsolutePath(), currentMime);			
		return request.sendMultipartForm(submissionForm).compose(response -> 
		{
			if(response.statusCode() >= 400)
			{
				return Future.failedFuture(new HttpException(response.statusCode(), response.bodyAsString()));
			}
			try {
				return Future.<JsonObject>succeededFuture(response.bodyAsJsonObject());
			}
			catch(Exception e)
			{
				LOGGER.debug(response.body());
				LOGGER.error(e, e);
				return Future.failedFuture(e);
			}			
		});	
	}
	
	public static Future<JsonObject> upload(WebClient client, String URL, JsonObject queryParameters,
			String fileName, File uploadFile)
	{
		return upload(client, URL, queryParameters, fileName, uploadFile, null);
	}
	
	public static Future<JsonObject> upload(WebClient client, String URL, JsonObject queryParameters, List<FileDescriptor> files)
	{
		HttpRequest<Buffer> request = client.post(URL);
		if(queryParameters != null)
		{
			for(String fieldname : queryParameters.fieldNames())
			{
				request.addQueryParam(fieldname, queryParameters.getString(fieldname));
			}
		}
		MultipartForm submissionForm = MultipartForm.create();
		for(FileDescriptor f : files)
		{			
				submissionForm.binaryFileUpload("files", f.getFileName(), f.getFilePath(), MimeMapping.mimeTypeForFilename(f.getFileName()));
		}
		return request.sendMultipartForm(submissionForm).compose(response -> 
		{
			if(response.statusCode() >= 400)
			{
				LOGGER.debug(response.bodyAsString());
				return Future.failedFuture(new HttpException(response.statusCode(), response.bodyAsString()));
			}
			try {
				return Future.<JsonObject>succeededFuture(response.bodyAsJsonObject());
			}
			catch(Exception e)
			{
				LOGGER.debug(response.body());
				LOGGER.error(e, e);
				return Future.failedFuture(e);
			}			
		});					
	}
	
	
	
	public Future<Void> authenticateSession(WebClientSession session, String username, String password)
	{
		MultiMap map = createFormFromJson(new JsonObject().put("username", username).put("password", password).put("remember", "1"));
		return POST(session, "/login", null, map).mapEmpty();		
	}
	/**
	 * Will authenticate the user with a token.
	 */
	public Future<Void> authenticateRequest(HttpRequest<Buffer> request, String username, String password)
	{
		return authenticateRequest(request, username, password, false, null);
	}

	public Future<Void> authenticateRequest(HttpRequest<Buffer> request, String username, String password, boolean createUser, Roles role )
	{
		Promise<Void> authPromise = Promise.promise();	
		// create 
		createUserForRequest(username, password, role, createUser)
		.compose(done -> {
			Promise<Void> authFinishedPromise = Promise.promise();
			authUser(username, password)
			.onSuccess( response -> {
				// now we have to set the right authentication header.
				request.bearerTokenAuthentication(response.bodyAsJsonObject().getString("token"));
				authFinishedPromise.complete();

			})
			.onFailure(err -> authFinishedPromise.fail(err));
			return authFinishedPromise.future();
		}).onSuccess(authed ->  {
			authPromise.complete();

		})
		.onFailure(err -> authPromise.fail(err));

		return authPromise.future();
	}

	public Future<HttpResponse<Buffer>> authUser(String username, String password)
	{
		Promise<HttpResponse<Buffer>> responsePromise = Promise.<HttpResponse<Buffer>>promise();
		MultiMap map = createFormFromJson(new JsonObject().put("username", username).put("password", password).put("remember", "0"));
		POST(webclient,"/login", null , map)
		.onSuccess(response -> {
			responsePromise.complete(response);
		})
		.onFailure(err -> responsePromise.fail(err));
		return responsePromise.future();
	}

	private Future<Void> createUserForRequest(String username, String password, Roles role,  boolean create)
	{
		if(!create)
		{
			return Future.succeededFuture();
		}
		else
		{					
			return createUser(vertx, username, password, role);
		}
	}

	protected Future<WebClientSession> createUserAndAuthedSession(String username, String password, Roles role)
	{
		WebClientSession currentSession = createSession();
		
		return createUser(vertx, username, password, role)
				.compose(userCreated -> { return authenticateSession(currentSession, username, password);})
				.compose(authed -> {return Future.succeededFuture(currentSession);});
	}
	
	protected Future<WebClientSession> createAuthedSession(String username, String password)
	{
		WebClientSession currentSession = createSession();
		return authenticateSession(currentSession, username, password)
				.compose(authed -> {return Future.succeededFuture(currentSession);});
	}
	
	protected Future<Void> submitResult(WebClient client, JsonObject resultData, String instanceID)
	{
		Promise<Void> submittedPromise = Promise.promise();
		POST(client, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
		.onSuccess(response -> {
			String taskInstanceID = response.bodyAsJsonObject().getString("id");			
			resultData.put("taskID",taskInstanceID);
			POST(client, "/study/" + instanceID + "/submit", null, resultData)
			.onSuccess(submitted -> {
				submittedPromise.complete();								
			})
			.onFailure(err -> submittedPromise.fail(err));
		})
		.onFailure(err -> submittedPromise.fail(err));

		return submittedPromise.future();
	}
}
