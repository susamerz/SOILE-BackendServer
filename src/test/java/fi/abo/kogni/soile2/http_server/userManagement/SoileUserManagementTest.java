package fi.abo.kogni.soile2.http_server.userManagement;

import java.net.HttpURLConnection;

import org.junit.Test;
import org.junit.runner.RunWith;

import fi.abo.kogni.soile2.http_server.SoileVerticleTest;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Unit tests for the User Management code (i.e. permission setting/removing, user addition etc pp)
 */
@RunWith(VertxUnitRunner.class)
public class SoileUserManagementTest extends SoileVerticleTest{

	@Test
	public void testUserAddition(TestContext context) {
		System.out.println("------------------------------- Testing User Addition-----------------------------------");
		Async async = context.async();
		try {
			createSoileUser("testUser", res -> {					
						if (res.succeeded())
						{									
							JsonObject obj = (JsonObject)res.result().body();					
							context.assertTrue(SoileCommUtils.isResultSuccessFull(obj));
							createSoileUser("testUser",invRes ->
								{
									if(invRes.succeeded())
									{
										context.fail("Could create user twice");
										async.complete();
									}
									else
									{							
										
										ReplyException ex = (ReplyException)invRes.cause();
										context.assertEquals(HttpURLConnection.HTTP_CONFLICT,ex.failureCode());
										context.assertEquals("User already exists", invRes.cause().getMessage());
										async.complete();																			
									}
								});													

						}					
						else
						{
							context.fail("No response found");
							async.complete();
						}
					});
		}
		catch(Exception e)
		{
			context.fail(e.getMessage());
			async.complete();
		}
	}

	@Test
	public void testRemoveUser(TestContext context) {
		System.out.println("--------------------  Testing User removal ----------------------");		 

		Async async = context.async();
		try {
			JsonObject userObject = new JsonObject()
					.put("username", "testUser")
					.put("password", "testpw")
					.put("email", "This@that.com")
					.put("type", "participant")
					.put("fullname","Test User");
			vertx.eventBus().request("soile.umanager.addUser", 
					userObject).onComplete(res -> {
						if (res.succeeded())
						{
							JsonObject obj = (JsonObject)res.result().body();					
							context.assertEquals("Success",obj.getValue("Result"));
							vertx.eventBus().request("soile.umanager.removeUser", userObject).andThen(remres ->
							{
								if(remres.succeeded())
								{
									Async iasync = context.async();
									//lets try to remove it again;
									vertx.eventBus().request("soile.umanager.removeUser", userObject).andThen( rrres ->
									{
										if(rrres.succeeded())
										{
											context.fail("Successfully removed already deleted user.");
											iasync.complete();
										}
										else
										{
											iasync.complete();	
										}
									});
									//Otherwise handle this.
									JsonObject resobj = (JsonObject) remres.result().body();
									{
										if(!SoileCommUtils.isResultSuccessFull(resobj)) {
											context.fail();
											async.complete();
										}
										else
										{
											async.complete();
										}
									}
								}
								else
								{									
									context.fail("Could not delete user");
									async.complete();
								}
							});
							
							
						}	
						else
						{
							context.fail("Could not re-add user");
							async.complete();
						}

					});
		}
		catch(Exception e)
		{
			context.fail();
			async.complete();
		}
	} 

	private Future<Message<Object>> createSoileUser(String username, Handler<AsyncResult<Message<Object>>> handler)
	{
		JsonObject userObject = new JsonObject()
				.put("username", username)
				.put("password", "testpw")
				.put("email", "This@that.com")
				.put("type", "participant")
				.put("fullname","Test User");
		return vertx.eventBus().request("soile.umanager.addUser", 
				userObject).onComplete(res -> {
					handler.handle(res);					
				});
	}
}
