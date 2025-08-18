package fi.abo.kogni.soile2.http_server;

import org.junit.Test;

import fi.abo.kogni.soile2.http_server.utils.DebugCookieStore;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.netty.handler.codec.http.cookie.Cookie;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.web.client.WebClientSession;

public class SoileLoginTest extends SoileVerticleTest {

	/**
	 * A Valid login attempt
	 * @param context
	 */
	@Test
	public void testValidAuthentication(TestContext context) {
		System.out.println("--------------------  Testing Valid Auth ----------------------");		 

		Async async = context.async();
		try {
			JsonObject userObject = new JsonObject()
					.put("username", "testUser")
					.put("password", "testpw")
					.put("email", "This@that.com")
					.put("type", "participant")
					.put("fullname","Test User")
					.put("remember","1");
			WebClientSession session = WebClientSession.create(webclient, new DebugCookieStore());			
			vertx.eventBus().request("soile.umanager.addUser", 
					userObject).andThen(res -> {
						if(res.succeeded())
						{
							//Now lets see if we can authenticate this.
							//This is a participant user, so we need to auth against the participant API
							MultiMap map = createFormFromJson(userObject);
							final Async sucAsync = context.async();		
							//Lets build a new session that we will attach the cookie to and try to restore this.
							WebClientSession newsession = createSession();								
							session.post(port,"localhost","/login").sendForm(map).onSuccess(authed ->
							{
								boolean foundSessionCookie = false;
								for(Cookie current : session.cookieStore().get(true, SoileConfigLoader.getServerProperty("domain"), SoileConfigLoader.getSessionProperty("cookiePath")))
								{
									if(current.name().equals(SoileConfigLoader.getSessionProperty("sessionCookieID")))
									{		
										//add the cookie to the other session.
										newsession.cookieStore().put(current);
										foundSessionCookie = true;
									}
								}
								context.assertTrue(foundSessionCookie);
								context.assertEquals(200,authed.statusCode());
								context.assertTrue(new JsonObject(authed.body().toString()).fieldNames().contains("token"));																		

								sucAsync.complete();	
								async.complete();
							});								
						}
						else
						{
							context.fail("Couldn't create user");
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

	/**
	 * A Valid login attempt
	 * @param context
	 */
	@Test
	public void testLogOut(TestContext context) {
		System.out.println("--------------------  Testing Logout----------------------");		 

		Async async = context.async();
		try {
			JsonObject userObject = new JsonObject()
					.put("username", "testUser")
					.put("password", "testpw")
					.put("email", "This@that.com")
					.put("type", "participant")
					.put("fullname","Test User")
					.put("remember","1");
			WebClientSession session = WebClientSession.create(webclient, new DebugCookieStore());			
			vertx.eventBus().request("soile.umanager.addUser", 
					userObject).andThen(res -> {
						if(res.succeeded())
						{
							//Now lets see if we can authenticate this.
							//This is a participant user, so we need to auth against the participant API
							MultiMap map = createFormFromJson(userObject);
							final Async sucAsync = context.async();		
							//Lets build a new session that we will attach the cookie to and try to restore this.
							WebClientSession newsession = createSession();								
							session.post(port,"localhost","/login").sendForm(map).onSuccess(authed ->
							{
								boolean foundSessionCookie = false;
								for(Cookie current : session.cookieStore().get(true, SoileConfigLoader.getServerProperty("domain"), SoileConfigLoader.getSessionProperty("cookiePath")))
								{
									if(current.name().equals(SoileConfigLoader.getSessionProperty("sessionCookieID")))
									{		
										//add the cookie to the other session.
										newsession.cookieStore().put(current);
										foundSessionCookie = true;
									}
								}
								Async auth_check = context.async();
								session.post(port,"localhost","/test/auth").send().onSuccess( authed_test -> {									
									JsonObject response = authed_test.bodyAsJsonObject();
									context.assertTrue(response.getBoolean("authenticated"));
									context.assertEquals(response.getString("user"),userObject.getString("username") );
									auth_check.complete();
								})
								.onFailure(err -> {									
									context.fail("Auth test failed");
									auth_check.complete();
								});;
								context.assertTrue(foundSessionCookie);
								context.assertEquals(200,authed.statusCode());
								context.assertTrue(new JsonObject(authed.body().toString()).fieldNames().contains("token"));
								Async logoutSync = context.async();
								newsession.post(port,"localhost","/logout").send()
								.onSuccess( not_authed -> {
									boolean foundNewCookie = false;
									boolean cookieExists = false;
									for(Cookie current : newsession.cookieStore().get(true, SoileConfigLoader.getServerProperty("domain"), SoileConfigLoader.getSessionProperty("cookiePath")))
									{
										if(current.name().equals(SoileConfigLoader.getSessionProperty("sessionCookieID")))
										{		
											//add the cookie to the other session.
											if(current.maxAge() > 0)
											{
												newsession.cookieStore().put(current);
												foundNewCookie = true;
											}					
											cookieExists = true;
										}
									}	
									context.assertFalse(foundNewCookie);
									context.assertTrue(cookieExists);
									context.assertEquals(200,not_authed.statusCode());
									Async auth_check2 = context.async();
									newsession.post(port,"localhost","/test/auth").send().onSuccess( authed_test -> {
										context.assertEquals(authed_test.statusCode(),401);						
										auth_check2.complete();
									})
									.onFailure(err -> {									
										context.fail("Auth test failed");
										auth_check2.complete();
									});
									logoutSync.complete();
								})
								.onFailure(err -> {									
									context.fail("logout failed");
									logoutSync.complete();
								});;								
								sucAsync.complete();	
								async.complete();
							});								
						}
						else
						{
							context.fail("Couldn't create user");
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


	/**
	 * A Valid login attempt
	 * @param context
	 */
	@Test
	public void testLoginWithJson(TestContext context) {
		System.out.println("--------------------  Testing Login with json ----------------------");		 

		Async async = context.async();
		try {
			JsonObject userObject = new JsonObject()
					.put("username", "testUser")
					.put("password", "testpw")
					.put("email", "This@that.com")
					.put("type", "participant")
					.put("fullname","Test User")
					.put("remember","1");
			WebClientSession session = WebClientSession.create(webclient, new DebugCookieStore());			
			vertx.eventBus().request("soile.umanager.addUser", 
					userObject).andThen(res -> {
						if(res.succeeded())
						{
							//Now lets see if we can authenticate this.
							//This is a participant user, so we need to auth against the participant API
							JsonObject login = new JsonObject().put("username", userObject.getValue("username"))
									.put("password", userObject.getValue("password"))
									.put("remember", "1");
							final Async sucAsync = context.async();		
							//Lets build a new session that we will attach the cookie to and try to restore this.
							WebClientSession newsession = createSession();								
							session.post(port,"localhost","/login").sendJsonObject(login).onSuccess(authed ->
							{
								boolean foundSessionCookie = false;
								for(Cookie current : session.cookieStore().get(true, SoileConfigLoader.getServerProperty("domain"), SoileConfigLoader.getSessionProperty("cookiePath")))
								{
									if(current.name().equals(SoileConfigLoader.getSessionProperty("sessionCookieID")))
									{		
										//add the cookie to the other session.
										newsession.cookieStore().put(current);
										foundSessionCookie = true;
									}
								}
								context.assertTrue(foundSessionCookie);
								context.assertEquals(200,authed.statusCode());
								context.assertTrue(new JsonObject(authed.body().toString()).fieldNames().contains("token"));																		
								session.post(hashingAlgo);
								sucAsync.complete();	
								async.complete();
							});								
						}
						else
						{
							context.fail("Couldn't create user");
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


	/**
	 * An invalid login attempt
	 * @param context
	 */
	@Test
	public void testInValidAuthentication(TestContext context) {
		System.out.println("--------------------  Testing Invalid Auth ----------------------");		 

		Async async = context.async();
		try {
			JsonObject userObject = new JsonObject()
					.put("username", "testUser")
					.put("password", "testpw")
					.put("email", "This@that.com")
					.put("type", "participant")
					.put("fullname","Test User")
					.put("remember","1");					
			vertx.eventBus().request("soile.umanager.addUser", 
					userObject).andThen(res -> {
						if(res.succeeded())
						{
							//Now lets see if we can authenticate this.
							//This is a participant user, so we need to auth against the participant API
							MultiMap map = createFormFromJson(userObject);																
							final Async unsucAsync = context.async();
							userObject.put("password", "user");
							map = createFormFromJson(userObject);
							webclient.post(port,"localhost","/login").sendForm(map).onSuccess(authed ->
							{
								context.assertTrue(authed.body().toString().contains("Unauthorized"));
								context.assertEquals(401,authed.statusCode());
								unsucAsync.complete();	
							});
							async.complete();
						}
						else
						{
							context.fail("Couldn't create user");
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
}
