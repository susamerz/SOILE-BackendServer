package fi.abo.kogni.soile2.http_server.routes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Test;

import fi.abo.kogni.soile2.datamanagement.DataLakeManagerTest;
import fi.abo.kogni.soile2.datamanagement.datalake.ParticipantFileResults;
import fi.abo.kogni.soile2.http_server.SoileWebTest;
import fi.abo.kogni.soile2.http_server.verticles.DataBundleGeneratorVerticle.DownloadStatus;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import fi.abo.kogni.soile2.utils.WebObjectCreator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.MimeMapping;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientSession;
import io.vertx.ext.web.handler.HttpException;

public class ParticipationRouterTest extends SoileWebTest{

	@Test
	public void getTaskInformationTest(TestContext context)
	{
		System.out.println("--------------------  Running get Task information test  ----------------------");    
		Async creationAsync = context.async();
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)
			.onSuccess(authToken -> {
				Async codeTypeAsync = context.async();
				WebClientSession tempSession = createSession();
				tempSession.addHeader("Authorization", authToken);
				POST(tempSession, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
				.onSuccess(inforesponse -> {
					JsonObject codeTypeInfo = inforesponse.bodyAsJsonObject();					
					context.assertEquals("qmarkup", codeTypeInfo.getJsonObject("codeType", new JsonObject()).getString("language"));
					context.assertEquals(1, codeTypeInfo.getJsonArray("outputs").size());
					context.assertEquals("smoker", codeTypeInfo.getJsonArray("outputs").getString(0));
					context.assertEquals(false, codeTypeInfo.getBoolean("finished") == null ? false : codeTypeInfo.getBoolean("finished"));
					POST(tempSession, "/study/" + instanceID + "/getcurrenttaskid", null, null)
					.onSuccess(idResponse -> {						
						String idInfo = idResponse.bodyAsString();
						context.assertEquals("tabcdefg0", idInfo);	
						codeTypeAsync.complete();
					})
					.onFailure(err -> context.fail(err));					
				})
				.onFailure(err -> context.fail(err));
				
				creationAsync.complete();
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}

	@Test
	public void signUpTest(TestContext context)
	{
		System.out.println("--------------------  Testing SignUp  ----------------------");    
		Async creationAsync = context.async();
		createResearcher(vertx, "TestUserForSignup", "password")
		.onSuccess(created -> {
			createAuthedSession("TestUserForSignup", "password")
			.onSuccess(authedSession -> {
				createAndStartTestProject(false)
				.onSuccess(instanceID -> {
					createTokenAndSignupUser(generatorSession, instanceID)
					.onSuccess(authToken -> {
						Async participantAsync = context.async();
						mongo_client.find(SoileConfigLoader.getdbProperty("studyCollection"), new JsonObject())
						.onSuccess(newresults -> {
						})
						.onFailure(err -> context.fail(err));
						mongo_client.find(SoileConfigLoader.getdbProperty("participantCollection"), new JsonObject())
						.onSuccess(results -> {

							context.assertEquals(1,results.size());
							context.assertEquals(authToken, results.get(0).getString("token"));
							String oldParticipantID = results.get(0).getString("_id"); 
							signUpToProject(authedSession, instanceID)
							.onSuccess(signedUp -> {
								mongo_client.find(SoileConfigLoader.getdbProperty("participantCollection"), new JsonObject())
								.onSuccess(newresults -> {
									context.assertEquals(2,newresults.size());
									for(JsonObject partData : newresults)
									{
										if(!oldParticipantID.equals(partData.getString("_id")))
										{
											// this has to be the new one.
											context.assertFalse(partData.containsKey("token"));
										}
									}
									participantAsync.complete();
								})
								.onFailure(err -> context.fail(err));						
							})
							.onFailure(err -> context.fail(err));	

						})
						.onFailure(err -> context.fail(err));							
						creationAsync.complete();
					})
					.onFailure(err -> context.fail(err));
				})
				.onFailure(err -> context.fail(err));
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}


	@Test
	public void runTaskTest(TestContext context)
	{
		System.out.println("--------------------  Running Task Tests  ----------------------");    
		Async creationAsync = context.async();
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)
			.onSuccess(authToken -> {
				Async codeTypeAsync = context.async();
				WebClientSession tempSession = createSession();
				tempSession.addHeader("Authorization", authToken);				
				POST(tempSession, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
				.onSuccess(inforesponse -> {
					JsonObject codeTypeInfo = inforesponse.bodyAsJsonObject();
					Async comparisonAsync = context.async();
					mongo_client.findOne(SoileConfigLoader.getCollectionName("participantCollection"), new JsonObject().put("token", authToken), null)
					.onSuccess(particpantData ->  {
						context.assertEquals(particpantData.getString("_id"), codeTypeInfo.getString("participantID"));	
						comparisonAsync.complete();
					})
					.onFailure(err -> context.fail(err));
					
					Async codeAsync = context.async();
					
					GET(tempSession, "/run/" + instanceID +"/" + codeTypeInfo.getString("id"), null, null)
					.onSuccess(response -> {
						context.assertEquals("application/json", response.headers().get("content-type"));					
						JsonObject compiledCode = response.bodyAsJsonObject();
						context.assertTrue(compiledCode.containsKey("elements"));
						context.assertEquals("html", compiledCode.getJsonArray("elements").getJsonArray(0).getJsonObject(0).getString("type"));
						context.assertEquals("",compiledCode.getString("title"));
						codeAsync.complete();
					})
					.onFailure(err -> context.fail(err));

					context.assertEquals("qmarkup", codeTypeInfo.getJsonObject("codeType", new JsonObject()).getString("language"));

					context.assertEquals(false, codeTypeInfo.getBoolean("finished") == null ? false : codeTypeInfo.getBoolean("finished"));
					codeTypeAsync.complete();
				})
				.onFailure(err -> context.fail(err));							
				creationAsync.complete();
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}



	// This test is more of a study test, but the helper functions are in here...
	@Test	
	public void runResetTest(TestContext context)
	{
		System.out.println("--------------------  Running Task Tests  ----------------------");    
		Async creationAsync = context.async();
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)
			.onSuccess(authToken -> {
				POST(generatorSession, "/study/" + instanceID + "/reset", null,null)
				.onSuccess(resetted -> {
					Async codeTypeAsync = context.async();
					WebClientSession tempSession = createSession();
					tempSession.addHeader("Authorization", authToken);
					POST(tempSession, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
					.onSuccess(inforesponse -> {
						context.fail("Should not be possible, since participant is no longer existing");						
					})
					.onFailure(err -> {
						HttpException e = (HttpException) err;
						// not authenticated, because the given participant no longer exists
						context.assertEquals(401, e.getStatusCode());
						codeTypeAsync.complete();
					});

					creationAsync.complete();
				})
				.onFailure(err -> context.fail(err));

			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}

	@Test	
	public void deleteStudyTest(TestContext context)
	{
		System.out.println("--------------------  Running Task Tests  ----------------------");    
		Async creationAsync = context.async();
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)
			.onSuccess(authToken -> {
				POST(generatorSession, "/study/" + instanceID + "/delete", null,null)
				.onSuccess(resetted -> {
					Async codeTypeAsync = context.async();
					WebClientSession tempSession = createSession();
					tempSession.addHeader("Authorization", authToken);
					POST(tempSession, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
					.onSuccess(inforesponse -> {
						context.fail("Should not be possible, since participant is no longer existing");						
					})
					.onFailure(err -> {
						HttpException e = (HttpException) err;
						// not authenticated, because the given participant no longer exists
						context.assertEquals(401, e.getStatusCode());
						codeTypeAsync.complete();
					});

					creationAsync.complete();
				})
				.onFailure(err -> context.fail(err));

			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}	


	@Test
	public void submitTest(TestContext context)
	{
		System.out.println("--------------------  Running Data Submission Tests  ----------------------");    

		Async creationAsync = context.async();
		JsonArray OutputData = new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 0)
				.put("timestamp", System.currentTimeMillis()));
		JsonObject resultData = new JsonObject().put("jsonData",new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 0)
				.put("timestamp", System.currentTimeMillis())
				)
				.add(new JsonObject().put("name", "smoker2")
						.put("value", "something")
						.put("timestamp", System.currentTimeMillis())
						)
				)
				.put("fileData", new JsonArray());
		JsonObject result = new JsonObject().put("outputData", OutputData).put("resultData", resultData);
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)			
			.onSuccess(authToken -> {
				Async submitAsync = context.async();
				WebClientSession tempSession = createSession();										
				tempSession.addHeader("Authorization", authToken);				
				submitResult(tempSession, result, instanceID)
				.onSuccess(done -> {
					getParticipantInfoForToken(authToken)
					.onSuccess(partInfo -> {
						context.assertNotNull(partInfo);
						// there is only one output
						context.assertEquals(OutputData,partInfo.getJsonArray("outputData").getJsonObject(0).getJsonArray("outputs"));
						submitAsync.complete();
					});
				})
				.onFailure(err -> context.fail(err));
				creationAsync.complete();
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}

	@Test
	public void dataUploadAndRetrievalTest(TestContext context)
	{
		System.out.println("--------------------  Running Data Upload and retrieval along with withdraw tests  ----------------------");    

		Async creationAsync = context.async();
		JsonArray OutputData = new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 0)
				.put("timestamp", System.currentTimeMillis()));
		JsonArray fileData = new JsonArray();
		JsonObject resultData = new JsonObject().put("jsonData",new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 0)
				.put("timestamp", System.currentTimeMillis())
				)
				.add(new JsonObject().put("name", "smoker2")
						.put("value", "something")
						.put("timestamp", System.currentTimeMillis())
						)
				)
				.put("fileData", fileData);

		JsonObject result = new JsonObject().put("outputData", OutputData).put("resultData", resultData);
		String TestDataFolder = WebObjectCreator.class.getClassLoader().getResource("FileTestData").getPath();
		String filename = "Image.jpg";
		File upload = new File(Path.of(TestDataFolder, "ImageData.jpg").toString());
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)			
			.onSuccess(authToken -> {
				Async submitAsync = context.async();
				WebClientSession tempSession = createSession();										
				tempSession.addHeader("Authorization", authToken);
				uploadResult(tempSession, instanceID, upload, filename, "image/jpeg")
				.onSuccess( uploadID -> {
					fileData.add(new JsonObject().put("fileformat", "image/jpeg")
							.put("filename", filename)
							.put("targetid", uploadID));
					submitResult(tempSession, result, instanceID)
					.onSuccess(done -> {
						Async dlNotReadyAsync = context.async();
						POST(generatorSession,"/study/" + instanceID + "/data", null, "" )
						.onSuccess(response -> {
							String dlID = response.bodyAsJsonObject().getString("downloadID");
							GET(generatorSession,"/study/" + instanceID + "/download/" + dlID, null, null)
							.onSuccess(res -> {
								// This is ok, the server was just really fast.
								dlNotReadyAsync.complete();
							})
							.onFailure(rejected -> {
								context.assertEquals(503, ((HttpException)rejected).getStatusCode());
								dlNotReadyAsync.complete();
							});					 
							awaitDownloadReady(generatorSession,instanceID,dlID, Promise.promise())
							.onSuccess(dlReady -> {
								GET(generatorSession,"/study/" + instanceID + "/download/" + dlID, null, null)
								.onSuccess(download -> {
									// This is ok, the server was just really fast.
									String targetFileName = tmpDir + File.separator + "WebTestdownload.tar.gz";
									vertx.fileSystem().writeFile(targetFileName , download.bodyAsBuffer())
									.onSuccess( dlSaved -> 
									{
										String targetDir = "/tmp" + File.separator + "WebTestdownload" + File.separator + "out";
										JsonObject DataJson = null; 
										try {
											unzip(targetFileName, targetDir);
											DataJson = new JsonObject(Files.readString(Path.of(targetDir,"data.json")));
											JsonArray taskResults = DataJson.getJsonArray("taskResults");
											JsonObject task1Res = null;
											for(int i = 0; i < taskResults.size(); ++i)
											{
												if(taskResults.getJsonObject(i).getString("taskID").equals("tabcdefg0"))
												{
													task1Res =  taskResults.getJsonObject(i).getJsonArray("resultData").getJsonObject(0);
													break;
												}

											}											

											context.assertEquals(1, task1Res.getInteger("step"));
											context.assertEquals(resultData.getValue("jsonData"), task1Res.getValue("dbData"));
											context.assertTrue(areFilesEqual(upload, new File(Path.of(targetDir, task1Res.getJsonArray("files").getJsonObject(0).getString("path")).toString())));

											Async withdrawAsync = context.async();
											// now, we withdraw the participant
											mongo_client.findOne(SoileConfigLoader.getCollectionName("participantCollection"),new JsonObject().put("token", authToken), null)
											.onSuccess(participantInfo -> {

												POST(tempSession,"/study/" + instanceID + "/withdraw", null, "" )
												.onSuccess(withdrawn -> {												
													String targetFileName2 = tmpDir + File.separator + "WebTestdownload2.tar.gz";
													String targetDir2 = "/tmp" + File.separator + "WebTestdownload2" + File.separator + "out";
													// let's check all places, where there should have been data:
													mongo_client.findOne(SoileConfigLoader.getCollectionName("participantCollection"),new JsonObject().put("token", authToken), null)
													.compose(partDeleted -> {
														context.assertNull(partDeleted);
														ParticipantFileResults results = new ParticipantFileResults(participantInfo.getString("_id"));
														return vertx.fileSystem().exists(results.getParticipantFolder());
													})
													.compose(exists -> {
														context.assertFalse(exists);
														return POST(generatorSession,"/study/" + instanceID + "/data/list", null, "" );															
													})
													.compose(dataList -> {
														context.assertEquals(0, dataList.bodyAsJsonObject().getJsonArray("participants").size());
														return POST(generatorSession,"/study/" + instanceID + "/data", null, "" );
													})
													.onSuccess(dataResponse -> {
														String dlID2 = dataResponse.bodyAsJsonObject().getString("downloadID");
														awaitDownloadReady(generatorSession,instanceID,dlID2, Promise.promise())
														.compose(ready -> {
															return GET(generatorSession,"/study/" + instanceID + "/download/" + dlID2, null, null);														
														})
														.compose(download2 ->  {														
															return vertx.fileSystem().writeFile(targetFileName2 , download2.bodyAsBuffer());
														})
														.onSuccess(dlfinished -> {
															JsonObject DataJson2 = null; 
															try {
																unzip(targetFileName2, targetDir2);
																DataJson2 = new JsonObject(Files.readString(Path.of(targetDir2,"data.json")));
																JsonArray taskResultData = DataJson2.getJsonArray("taskResults");
																for(int i = 0; i < taskResultData.size(); i++)
																{
																	context.assertEquals(0, taskResultData.getJsonObject(i).getJsonArray("resultData").size());	
																}																																
																withdrawAsync.complete();
															}
															catch(Exception e)
															{
																context.fail(e);
															}
														})
														.onFailure(err -> context.fail(err));														
													})
													.onFailure(err -> context.fail(err));

													/*
												POST(generatorSession,"/study/" + instanceID + "/data", null, "" )
												.onSuccess(response2 -> {
													
												})
												.onFailure(err -> context.fail(err));*/

												})
												.onFailure(err -> context.fail(err));

											})
											.onFailure(err -> context.fail(err));

											submitAsync.complete();
										}
										catch(Exception e)
										{
											context.fail(e);
										}										

										//submitAsync.complete();
									})
									.onFailure(err -> context.fail(err));	

								})
								.onFailure(err -> context.fail(err));

							})
							.onFailure(err -> context.fail(err));	

						})
						.onFailure(err -> context.fail(err));
						Async invalidRequestAsync = context.async();
						POST(tempSession,"/study/" + instanceID + "/data", null, "" )
						.onSuccess(fail -> {
							context.fail("Invalid access should not be possible");
						})
						.onFailure(err -> invalidRequestAsync.complete());

					})
					.onFailure(err -> context.fail(err));
				})
				.onFailure(err -> context.fail(err));
				creationAsync.complete();
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}


	@Test
	public void projectTest(TestContext context)
	{
		System.out.println("--------------------  Running Web Project Progression Test  ----------------------");    

		Async creationAsync = context.async();
		JsonArray OutputData = new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 1)
				.put("timestamp", System.currentTimeMillis()));
		JsonArray fileData = new JsonArray();
		JsonObject resultData = new JsonObject().put("jsonData",new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 1)
				.put("timestamp", System.currentTimeMillis())
				)
				.add(new JsonObject().put("name", "smoker2")
						.put("value", "something")
						.put("timestamp", System.currentTimeMillis())
						)
				)
				.put("fileData", fileData);

		JsonObject result = new JsonObject().put("outputData", OutputData).put("persistentData",OutputData).put("resultData", resultData);		
		String TestDataFolder = WebObjectCreator.class.getClassLoader().getResource("FileTestData").getPath();
		File upload = new File(Path.of(TestDataFolder, "textData.txt").toString());
		List<File> fileUploads = new LinkedList<>();
		fileUploads.add(upload);
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)			
			.onSuccess(authToken -> {
				Async submitAsync = context.async();
				WebClientSession tempSession = createSession();				
				tempSession.addHeader("Authorization", authToken);					
				submitFilesAndResults(tempSession, fileUploads, result.copy(), instanceID)
				.onSuccess(submitted -> {
					Async persistentAsync = context.async();
					POST(tempSession,"/study/" + instanceID + "/getpersistent", null, null)
					.onSuccess( response -> {
						JsonObject responsebody = response.bodyAsJsonObject();
						context.assertTrue(responsebody.containsKey("smoker"));
						context.assertEquals(1, responsebody.getNumber("smoker"));
						persistentAsync.complete();						
					})				
					.onFailure(err -> context.fail(err));
					submitFilesAndResults(tempSession, fileUploads, result.copy().put("outputData",  new JsonArray().add(new JsonObject().put("name",  "clicktimes").put("value", 0.5))), instanceID)
					.onSuccess(submitted2 -> {
						POST(tempSession,"/study/" + instanceID + "/getcurrenttaskinfo", null, null)
						.onSuccess(inforesponse -> {
							GET(tempSession, "/run/" + instanceID + "/" + inforesponse.bodyAsJsonObject().getString("id"), null, null)
							.onSuccess(code -> {
								context.assertEquals("application/javascript", code.headers().get("content-type"));					
								submitFilesAndResults(tempSession, fileUploads, result.copy(), instanceID)
								.onSuccess(submitted3 -> {
									submitFilesAndResults(tempSession, fileUploads, result.copy(), instanceID)
									.onSuccess(submitted4 -> {
										submitFilesAndResults(tempSession, fileUploads, result.copy(), instanceID)
										.onSuccess(submitted5 -> {
											POST(tempSession, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
											.onSuccess(response -> {									
												JsonObject finalresult = response.bodyAsJsonObject();
												// now the project is done, we have passed filters and everything. 
												context.assertTrue(finalresult.getBoolean("finished"));
												submitAsync.complete();
											})
											.onFailure(err -> context.fail(err));
										})
										.onFailure(err -> context.fail(err));
									})
									.onFailure(err -> context.fail(err));
								})
								.onFailure(err -> context.fail(err));
							})
							.onFailure(err -> context.fail(err));
						})
						.onFailure(err -> context.fail(err));
					})
					.onFailure(err -> context.fail(err));
				})
				.onFailure(err -> context.fail(err));
				creationAsync.complete();
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}


	@Test
	public void testPersistentData(TestContext context)
	{
		System.out.println("--------------------  Running Persistent Data Test  ----------------------");    

		Async creationAsync = context.async();
		JsonArray OutputData = new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 1)
				.put("timestamp", System.currentTimeMillis()));
		JsonArray fileData = new JsonArray();
		JsonObject resultData = new JsonObject().put("jsonData",new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 1)
				.put("timestamp", System.currentTimeMillis())
				)
				.add(new JsonObject().put("name", "smoker2")
						.put("value", "something")
						.put("timestamp", System.currentTimeMillis())
						)
				)
				.put("fileData", fileData);

		JsonObject result = new JsonObject().put("outputData", OutputData).put("persistentData",OutputData).put("resultData", resultData);		
		String TestDataFolder = WebObjectCreator.class.getClassLoader().getResource("FileTestData").getPath();
		File upload = new File(Path.of(TestDataFolder, "textData.txt").toString());
		List<File> fileUploads = new LinkedList<>();
		fileUploads.add(upload);
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)			
			.onSuccess(authToken -> {
				Async submitAsync = context.async();
				WebClientSession tempSession = createSession();				
				tempSession.addHeader("Authorization", authToken);
				POST(tempSession,"/study/" + instanceID + "/getpersistent", null, null)
				.onSuccess( persistentDataresponse -> {
					context.assertTrue(persistentDataresponse.bodyAsJsonObject().isEmpty());

					submitFilesAndResults(tempSession, fileUploads, result.copy(), instanceID)
					.onSuccess(submitted -> {
						Async persistentAsync = context.async();
						POST(tempSession,"/study/" + instanceID + "/getpersistent", null, null)
						.onSuccess( response -> {
							JsonObject responsebody = response.bodyAsJsonObject();
							context.assertTrue(responsebody.containsKey("smoker"));
							context.assertEquals(1, responsebody.getNumber("smoker"));
							persistentAsync.complete();						
						})				
						.onFailure(err -> context.fail(err));
						JsonObject newResults = result.copy().put("outputData",  new JsonArray().add(new JsonObject().put("name",  "clicktimes").put("value", 0.5)));
						newResults.put("persistentData", new JsonArray().add(new JsonObject().put("name", "smoker")
								.put("value", 0)
								.put("timestamp", System.currentTimeMillis())));
						submitFilesAndResults(tempSession, fileUploads, newResults , instanceID)
						.onSuccess(submitted2 -> {

							POST(tempSession,"/study/" + instanceID + "/getpersistent", null, null)
							.onSuccess( response -> {
								JsonObject responsebody = response.bodyAsJsonObject();
								context.assertTrue(responsebody.containsKey("smoker"));
								context.assertEquals(0, responsebody.getNumber("smoker"));
								submitAsync.complete();
							})				
							.onFailure(err -> context.fail(err));

						})
						.onFailure(err -> context.fail(err));
					})
					.onFailure(err -> context.fail(err));
				})
				.onFailure(err -> context.fail(err));
				creationAsync.complete();
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}

	@Test
	public void testRunResources(TestContext context)
	{
		System.out.println("--------------------  Running Test for /run/{id}/*  ----------------------");    

		JsonArray OutputData = new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 1)
				.put("timestamp", System.currentTimeMillis()));
		JsonArray fileData = new JsonArray();
		JsonObject resultData = new JsonObject().put("jsonData",new JsonArray().add(new JsonObject().put("name", "smoker")
				.put("value", 1)
				.put("timestamp", System.currentTimeMillis())
				)
				.add(new JsonObject().put("name", "smoker2")
						.put("value", "something")
						.put("timestamp", System.currentTimeMillis())
						)
				)
				.put("fileData", fileData);

		JsonObject result = new JsonObject().put("outputData", OutputData).put("resultData", resultData);
		String TestDataFolder = WebObjectCreator.class.getClassLoader().getResource("FileTestData").getPath();
		File upload = new File(Path.of(TestDataFolder, "textData.txt").toString());
		List<File> fileUploads = new LinkedList<>();
		fileUploads.add(upload);

		Async testRunResource = context.async();
		createAndStartTestProject(true, "testProject")
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)
			.onSuccess(authToken -> {
				WebClientSession tempSession = createSession();				
				tempSession.addHeader("Authorization", authToken);
				POST(tempSession,"/study/" + instanceID + "/getcurrenttaskinfo", null, null)
				.onSuccess(inforesponse -> {
					GET(tempSession, "/run/" + instanceID + "/" + inforesponse.bodyAsJsonObject().getString("id") +  "/ImageData.jpg", null, null)
					.onSuccess(failed -> {
						context.fail("The first Task does NOT have any resources!");
					})
					.onFailure(response -> {
						context.assertEquals(404, ((HttpException)response).getStatusCode());
						submitFilesAndResults(tempSession, fileUploads, result.copy(), instanceID)
						.onSuccess(submitted -> {
							POST(tempSession,"/study/" + instanceID + "/getcurrenttaskinfo", null, null)
							.onSuccess(inforesponse2 -> {
								GET(tempSession, "/run/testProject" + "/" + inforesponse2.bodyAsJsonObject().getString("id") +"/ImageData.jpg", null, null)
								.onSuccess(dataResponse ->  {
									String targetFileName = tmpDir + File.separator + "taskRouter.out"; 
									vertx.fileSystem().writeFile(targetFileName , dataResponse.bodyAsBuffer())
									.onSuccess(res -> {
										// compare that the file is the same as the original one.
										try {
											context.assertTrue( 
													areFilesEqual(
															new File(targetFileName),
															new File(DataLakeManagerTest.class.getClassLoader().getResource("FileTestData/ImageData.jpg").getPath())
															)
													);
											testRunResource.complete();

										}
										catch(IOException e)
										{
											context.fail(e);
										}
									})					 
									.onFailure(err -> context.fail(err));
								})
								.onFailure(err -> context.fail(err));
							})					 
							.onFailure(err -> context.fail(err));

						})
						.onFailure(err -> context.fail(err));
					});					

				})
				.onFailure(err -> context.fail(err));
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}

	@Test
	public void testRunLibs(TestContext context)
	{
		System.out.println("--------------------  Running Tests for /run/{id}/{taskID}/lib/*  ----------------------");    

		Async testRunResource = context.async();
		createAndStartTestProject(true)
		.onSuccess(instanceID -> {
			createTokenAndSignupUser(generatorSession, instanceID)
			.onSuccess(authToken -> {
				WebClientSession tempSession = createSession();				
				tempSession.addHeader("Authorization", authToken);
				POST(tempSession,"/study/" + instanceID + "/getcurrenttaskinfo", null, null)
				.onSuccess(inforesponse -> {
					GET(tempSession, "/run/" + instanceID + "/" + inforesponse.bodyAsJsonObject().getString("id") + "/lib/testlib.js", null, null)
					.onSuccess(response -> {
						context.assertTrue(response.bodyAsString().contains("console.log"));
						testRunResource.complete();
					})
					.onFailure(err -> context.fail(err));
					Async failedLibAsync = context.async();
					GET(tempSession, "/run/" + instanceID + "/" + inforesponse.bodyAsJsonObject().getString("id") + "/lib/testlib2.js", null, null)
					.onSuccess(response -> {
						context.fail("Should not be possible");
					})
					.onFailure(err ->
					{
						if( err instanceof HttpException)
						{
							context.assertEquals(404, ((HttpException)err).getStatusCode());
							failedLibAsync.complete();
						}
						else
						{
							context.fail(err);
						}

					});
				})
				.onFailure(err -> context.fail(err));
			})
			.onFailure(err -> context.fail(err));
		})
		.onFailure(err -> context.fail(err));
	}



	/**
	 * Submit files and results with the given webclient(session). 
	 * @param client
	 * @param uploads
	 * @param resultData
	 * @param instanceID
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	protected Future<Void> submitFilesAndResults(WebClient client, List<File> uploads, JsonObject resultData, String instanceID)
	{
		Promise<Void> submittedPromise = Promise.promise();
		List<Future<Void>> submissionFutures = new LinkedList<Future<Void>>();
		ConcurrentLinkedDeque<JsonObject> uploadObjects = new ConcurrentLinkedDeque<>();
		for(File f : uploads)
		{		
			Promise done = Promise.promise();
			submissionFutures.add(done.future().mapEmpty());
			uploadResult(client, instanceID, f, f.getName(), MimeMapping.mimeTypeForFilename(f.getName()))
			.onSuccess(id -> {
				uploadObjects.add(new JsonObject().put("fileformat", MimeMapping.mimeTypeForFilename(f.getName()))
						.put("filename", f.getName())
						.put("targetid", id));				
				done.complete();

			})
			.onFailure(err -> done.fail(err));
		}
		Future.all(submissionFutures).onSuccess(allSubmitted -> {
			POST(client, "/study/" + instanceID + "/getcurrenttaskinfo", null, null)
			.onSuccess(response -> {
				JsonArray fileResults = resultData.getJsonObject("resultData").getJsonArray("fileData");
				for(JsonObject o : uploadObjects)
				{
					fileResults.add(o);
				}

				String taskInstanceID = response.bodyAsJsonObject().getString("id");				
				resultData.put("taskID",taskInstanceID);
				POST(client, "/study/" + instanceID + "/submit", null, resultData)
				.onSuccess(submitted -> {
					submittedPromise.complete();								
				})
				.onFailure(err -> submittedPromise.fail(err));
			})
			.onFailure(err -> submittedPromise.fail(err));
		})
		.onFailure(err -> submittedPromise.fail(err));

		return submittedPromise.future();
	}



	protected Future<JsonObject> getParticipantInfoForUsername(String username, String projectID)
	{
		Promise<JsonObject> participantPromise = Promise.promise();
		mongo_client.findOne(SoileConfigLoader.getCollectionName("userCollection"),
				new JsonObject().put(SoileConfigLoader.getMongoAuthNOptions().getUsernameField(), username), 
				new JsonObject().put(SoileConfigLoader.getUserdbField("participantField"),1 ))
		.onSuccess(participantJson -> {
			JsonArray participantInfo = participantJson.getJsonArray(SoileConfigLoader.getUserdbField("participantField"), new JsonArray());
			String participantID = null;
			for(int i = 0; i < participantInfo.size(); i++)
			{
				// TODO: Use an aggregation to retrieve this (that's probably faster).
				if(participantInfo.getJsonObject(i).getString("UUID").equals(projectID))
				{
					participantID = participantInfo.getJsonObject(i).getString("participantID");
					break;
				}

			}
			if(participantID != null)
			{
				mongo_client.findOne(SoileConfigLoader.getCollectionName("participantCollection"), new JsonObject().put("_id", participantID), null)
				.onSuccess(result ->
				{
					participantPromise.complete(result);
				})
				.onFailure(err -> participantPromise.fail(err));
			}
			else
			{
				participantPromise.fail("No participant for user in project");
			}

		})
		.onFailure(err -> participantPromise.fail(err));

		return participantPromise.future();		
	}

	/**
	 * Get the participant information for a Token directly from the database.
	 * @param token
	 * @return
	 */
	protected Future<JsonObject> getParticipantInfoForToken(String token)
	{			
		return mongo_client.findOne(SoileConfigLoader.getCollectionName("participantCollection"), new JsonObject().put("token", token), null);		
	}

	private Future<Void> awaitDownloadReady(WebClient client, String projectID, String dlID, Promise<Void> readyPromise)
	{		
		POST(client, "/study/" + projectID + "/download/" + dlID + "/check", null, null).onSuccess(response -> 
		{
			JsonObject status = response.bodyAsJsonObject();
			if(status.getString("status").equals(DownloadStatus.downloadReady.toString()))
			{
				readyPromise.complete();
				return;
			}
			else
			{
				if(status.getString("status").equals(DownloadStatus.failed.toString()))
				{
					readyPromise.fail(new Exception("Download Failed"));
				}
				else
				{
					try {
						// we need to wait a tiny bit... 
						TimeUnit.MILLISECONDS.sleep(50);
						awaitDownloadReady(client,projectID, dlID, readyPromise);
					}
					catch(InterruptedException e)
					{
						readyPromise.fail(e);
					}
				}
			}											
		});
		return readyPromise.future();
	}

	private void unzip(String zipFilePath, String destDir) throws Exception{
		File dir = new File(destDir);
		// create output directory if it doesn't exist
		if(!dir.exists()) dir.mkdirs();
		FileInputStream fis;
		//buffer for read and write data to file
		byte[] buffer = new byte[1024];

		fis = new FileInputStream(zipFilePath);
		ZipInputStream zis = new ZipInputStream(fis);
		ZipEntry ze = zis.getNextEntry();
		while(ze != null){
			String fileName = ze.getName();
			File newFile = new File(destDir + File.separator + fileName);
			//create directories for sub directories in zip
			new File(newFile.getParent()).mkdirs();
			FileOutputStream fos = new FileOutputStream(newFile);
			int len;
			while ((len = zis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
			//close this ZipEntry
			zis.closeEntry();
			ze = zis.getNextEntry();
		}
		//close last ZipEntry
		zis.closeEntry();
		zis.close();
		fis.close();

	}

}
