package fi.abo.kogni.soile2.http_server.authentication;

import org.junit.Test;
import org.junit.runner.RunWith;

import fi.abo.kogni.soile2.MongoTest;
import fi.abo.kogni.soile2.UserManagementTest;
import fi.abo.kogni.soile2.http_server.auth.SoileAuthentication;
import fi.abo.kogni.soile2.http_server.userManagement.SoileUserManager;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SoileAuthenticationTest extends MongoTest implements UserManagementTest{

	SoileUserManager uManager;
	
	@Override
	public void runBeforeTests(TestContext context)
	{
		super.runBeforeTests(context);
		uManager = createManager(vertx);
	}
	
	
	@SuppressWarnings("deprecation")
	@Test	
	public void testValidAuthentication(TestContext context) {
		System.out.println("--------------------  Testing Valid Auth ----------------------");		 

		// create a user, that we want to authenticate later on.
		JsonObject participant1 = new JsonObject().put("username", "participant")
				.put(SoileConfigLoader.getSessionProperty("passwordField"), "password");


		JsonObject DB_User = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), "participant")
				.put(SoileConfigLoader.getUserdbField("passwordField"), "password"); 
		Async overall = context.async();
		// Lets create two users.
		createUser(DB_User, context, uManager ).onComplete(x1 -> 
		{			

			SoileAuthentication auth = new SoileAuthentication(mongo_client);		
			final Async async = context.async();
			// test auth
			
			auth.authenticate(new UsernamePasswordCredentials(participant1)).onComplete( user -> {
				if(user.succeeded())
				{
					context.assertEquals(participant1.getString("username"),
							user.result().principal().getString("username"));
					async.complete();
				}			
				else
				{
					context.fail("Could not authenticate");
					async.complete();
				}
			});
			overall.complete();
		});		
	}

	@SuppressWarnings("deprecation")
	@Test	
	public void testInValidAuthentication(TestContext context) {
		System.out.println("--------------------  Testing Invalid Auth ----------------------");		 

		// create a user, that we want to authenticate later on.

		JsonObject invalidUser = new JsonObject().put("username", "user1")
				.put(SoileConfigLoader.getSessionProperty("passwordField"), "password2");

		JsonObject DB_User = new JsonObject().put(SoileConfigLoader.getUserdbField("usernameField"), "participant")
				.put(SoileConfigLoader.getUserdbField("passwordField"), "password"); 

		
		Async overall = context.async();
		// Lets create two users.
		createUser(DB_User, context, uManager).onComplete(x1 -> 
		{					

			SoileAuthentication auth = new SoileAuthentication(mongo_client);				
			final Async invalidAuth = context.async();
			auth.authenticate(new UsernamePasswordCredentials(invalidUser)).onComplete( user -> {
				if(user.succeeded())
				{				
					context.fail("Invalid authentication succeeded");
					invalidAuth.complete();
				}
				else
				{
					context.assertEquals("Invalid username or invalid password for username: user1", user.cause().getMessage());
					invalidAuth.complete();
				}
			});
			overall.complete();
		});
	}
}
