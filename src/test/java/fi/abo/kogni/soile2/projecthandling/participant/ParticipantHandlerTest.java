package fi.abo.kogni.soile2.projecthandling.participant;

import java.io.IOException;
import java.util.LinkedList;

import org.junit.Test;

import fi.abo.kogni.soile2.GitTest;
import fi.abo.kogni.soile2.projecthandling.ProjectBaseTest;
import fi.abo.kogni.soile2.projecthandling.exceptions.InvalidPositionException;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.ElementManager;
import fi.abo.kogni.soile2.projecthandling.projectElements.impl.Project;
import fi.abo.kogni.soile2.projecthandling.projectElements.instance.impl.StudyHandler;
import fi.abo.kogni.soile2.projecthandling.utils.ObjectGenerator;
import fi.abo.kogni.soile2.projecthandling.utils.ProjectFactoryImplForTesting;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class ParticipantHandlerTest extends GitTest{

	@SuppressWarnings("rawtypes")
	@Test
	public void testGetParticipantStatus(TestContext context) {
		System.out.println("--------------------  Testing Participant Status retrieval ----------------------");
		StudyHandler projHandler = new StudyHandler(mongo_client, vertx);
		ParticipantHandler partHandler = new ParticipantHandler(mongo_client, projHandler, vertx);
		Async testAsync = context.async();
		ObjectGenerator.buildAPIProject(ElementManager.getProjectManager(mongo_client, vertx), ElementManager.getExperimentManager(mongo_client, vertx), ElementManager.getTaskManager(mongo_client, vertx), mongo_client, "Testproject")
		.onSuccess(apiProject-> {
			JsonObject studyProps = new JsonObject().put("name", "testStudy")
													.put("sourceProject", apiProject.getAPIJson())
													.put("private", false);
			projHandler.createStudy(studyProps)
			.onSuccess(projectInstance -> {
				projectInstance.activate()
				.onSuccess(active -> {
					LinkedList<Future<Void>> participantFutures = new LinkedList<Future<Void>>();
					participantFutures.add(partHandler.create(projectInstance.getID()).mapEmpty());
					participantFutures.add(partHandler.create(projectInstance.getID()).mapEmpty());
					participantFutures.add(partHandler.create(projectInstance.getID()).mapEmpty());
					Future.all(participantFutures).mapEmpty()
					.onSuccess(res -> {
						partHandler.getParticipantStatusForProject(projectInstance)
						.onSuccess(participantInfo -> 
						{
							context.assertEquals(3, participantInfo.size());
							for(int i = 0; i < participantInfo.size(); ++i)
							{
								context.assertFalse(participantInfo.getJsonObject(i).getBoolean("finished"));
							}
							testAsync.complete();
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

	}


	@Test
	public void testParticipantStatus(TestContext context) {
		System.out.println("--------------------  Testing Participnt Status settings ----------------------");
		StudyHandler projHandler = new StudyHandler(mongo_client, vertx);
		ParticipantHandler partHandler = new ParticipantHandler(mongo_client, projHandler, vertx);
		JsonObject smokerOutput = new JsonObject()
				.put("name", "smoker")
				.put("value", 1);


		JsonObject wrongquestionaireOutput = new JsonObject().put("outputData", new JsonArray().add(smokerOutput)).put("taskID", "t83297d7785fd249bdb6543a850680e812ce11873df2d48467cb9612dbd0482b2"); 
		JsonObject smokerQuestionaireOutput = new JsonObject().put("persistentData", new JsonArray().add(smokerOutput)).put("outputData", new JsonArray().add(smokerOutput)).put("taskID", "t83297d7785fd249bdb6543a850680e812ce11873df2d48467cb9612dbd0482b1");
		Async participantAsync = context.async();
		try
		{
			ProjectFactoryImplForTesting.loadProject(ProjectBaseTest.getPos(0))
			.onSuccess(proj -> {
				partHandler.create(proj)
				.onSuccess(participant -> {
					Async projTestAsync = context.async();
					proj.startStudy(participant)
					.onSuccess(v1 -> {
						context.assertEquals("t83297d7785fd249bdb6543a850680e812ce11873df2d48467cb9612dbd0482b1", participant.getStudyPosition());
						Async invalidAsync = context.async();
						proj.finishStep(participant, wrongquestionaireOutput).
						onSuccess(r -> {
							context.fail("This should be unreachable as the previouscommand should throw an exception");	
						})
						.onFailure(err -> {
							context.assertEquals(InvalidPositionException.class, err.getClass());
							invalidAsync.complete();
						});

						proj.finishStep(participant, smokerQuestionaireOutput)
						.onSuccess(id -> {
							context.assertEquals("t83297d7785fd249bdb6543a850680e812ce11873df2d48467cb9612dbd0482b2", participant.getStudyPosition());
							proj.finishStep(participant, new JsonObject().put("taskID", id))
							.onSuccess(newID -> {								
								context.assertEquals("t83297d7785fd249bdb6543a850680e812ce11873df2d48467cb9612dbd0482b4", newID);
								partHandler.getParticipantStatusForProject(proj)
								.onSuccess(resultArray -> {
									context.assertEquals(1, resultArray.size());
									context.assertFalse(resultArray.getJsonObject(0).getBoolean("finished"));
									context.assertEquals(participant.getID(), resultArray.getJsonObject(0).getString("participantID"));
									proj.finishStep(participant, new JsonObject().put("taskID", newID))
									.onSuccess(newID2 -> {
										// the next step from the first experiment is skipped because it filters for smoker = 0, so we are in the next experiment.
										LinkedList<String> resultOptions = new LinkedList<String>();
										resultOptions.add("t83297d7785fd249bdb6543a850680e812ce11873df2d48467cb9612dbd0482b7");
										resultOptions.add("t83297d7785fd249bdb6543a850680e812ce11873df2d48467cb9612dbd0482b8");
										context.assertTrue(resultOptions.contains(newID2));
										proj.finishStep(participant, new JsonObject().put("taskID", newID2))
										.onSuccess(newID3 -> {										
											context.assertTrue(resultOptions.contains(newID2));
											context.assertNotEquals(newID2, newID3);
											proj.finishStep(participant, new JsonObject().put("taskID", newID3))
											.onSuccess(newID4 -> {
												context.assertTrue(newID4 == null);	
												mongo_client.findOne(SoileConfigLoader.getCollectionName("participantCollection"),new JsonObject().put("_id", participant.getID()),null)
												.onSuccess( json -> {
													context.assertTrue(participant.isFinished());
													context.assertTrue(json.getBoolean("finished"));
													context.assertEquals(6,json.getInteger("currentStep"));
													partHandler.getParticipantStatusForProject(proj)
													.onSuccess(resultArray2 -> {
														context.assertEquals(1, resultArray2.size());
														context.assertTrue(resultArray2.getJsonObject(0).getBoolean("finished"));
														context.assertEquals(participant.getID(), resultArray2.getJsonObject(0).getString("participantID"));
														projTestAsync.complete();	

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
					})
					.onFailure(err -> context.fail(err));
					participantAsync.complete();
				})
				.onFailure(err -> context.fail(err));					
			})
			.onFailure(err -> context.fail(err));
		}
		catch(IOException e)
		{
			context.fail(e);
		}
	}


	@Test
	public void testCreateAndGet(TestContext context) {
		System.out.println("--------------------  Testing Participant Creating and retrieval ----------------------");
		StudyHandler projHandler = new StudyHandler(mongo_client, vertx);
		ParticipantHandler partHandler = new ParticipantHandler(mongo_client, projHandler, vertx);
		Async testAsync = context.async();
		JsonObject smokerOutput = new JsonObject()
				.put("name", "smoker")
				.put("value", 1);
		JsonObject smokerQuestionaireOutput = new JsonObject().put("persistentData", new JsonArray().add(smokerOutput)).put("outputData", new JsonArray().add(smokerOutput));

		ElementManager<Project> projectManager = ElementManager.getProjectManager(mongo_client, vertx);
		ObjectGenerator.buildAPIProject(projectManager, ElementManager.getExperimentManager(mongo_client, vertx), ElementManager.getTaskManager(mongo_client, vertx), mongo_client, "Testproject2")
		.onSuccess(apiProject-> {
			JsonObject studyProps = new JsonObject().put("name", "testStudy")
					.put("sourceProject", apiProject.getAPIJson())
					.put("private", false);
			projHandler.createStudy(studyProps)
			.onSuccess(projectInstance -> {		
				projectInstance.activate()
				.onSuccess(active -> {
					projectManager.getGitJson(apiProject.getUUID(), apiProject.getVersion())
					.onSuccess(gitJson -> {
						partHandler.create(projectInstance.getID())				
						.onSuccess( participant -> 
						{				
							projectInstance.startStudy(participant)
							.onSuccess(position -> {
								partHandler.getParticipant(participant.getID())
								.onSuccess(participant2 -> 
								{
									context.assertEquals(participant, participant2);
									context.assertFalse(participant == participant2);
									partHandler.getParticipant(participant.getID())
									.onSuccess(participant3 -> 
									{
										context.assertEquals(participant, participant3);
										// those should be the very same object as retrieved by the participantHandler.
										context.assertTrue(participant2 == participant3);
										context.assertFalse(participant == participant3);
										//
										projectInstance.finishStep(participant, smokerQuestionaireOutput.put("taskID", position))
										.onSuccess(res -> {
											partHandler.getParticipant(participant.getID())
											.onSuccess(participant4 ->
											{
												//this should be a new object, as the participant should have been made "dirty"
												context.assertEquals(participant, participant4);
												// those should be the very same object as retrieved by the participantHandler.									
												context.assertFalse(participant == participant4);
												context.assertFalse(participant2 == participant4);
												testAsync.complete();			
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
			})
			.onFailure(err -> context.fail(err));

		})
		.onFailure(err -> context.fail(err));

	}
}
