package fi.abo.kogni.soile2.http_server;

import org.junit.Test;
import org.junit.runner.RunWith;

import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.Roles;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.netty.handler.codec.http.cookie.Cookie;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClientSession;


@RunWith(VertxUnitRunner.class)
public class SoileauthCheckTest extends SoileVerticleTest{	
	

	@Test
	public void testAuthWorks(TestContext context) {
		System.out.println("***************************************** Starting Authentication Check Test *************************************************");
		Async userCreationAsync = context.async();
		try {
			JsonObject userObject = new JsonObject()
					.put("username", "testUser")
					.put("password", "testpw")
					.put("email", "This@that.com")
					.put("type", "participant")
					.put("fullname","Test User")
					.put("remember","1");
			WebClientSession session = createSession();			
			vertx.eventBus().request("soile.umanager.addUser", 
					userObject).andThen(res -> {
						if(res.succeeded())
						{
							//Now lets see if we can authenticate this.
							//This is a participant user, so we need to auth against the participant API
							MultiMap map = createFormFromJson(userObject);
							Async login = context.async();		
							//Lets build a new session that we will attach the cookie to and try to restore this.
							WebClientSession newCookieSession = createSession();
							WebClientSession newTokenSession = createSession();
							WebClientSession unregisteredSession = createSession();								
							session.post(port,"localhost","/login").sendForm(map).onSuccess(authed ->
							{
								boolean foundSessionCookie = false;
								for(Cookie current : session.cookieStore().get(true, SoileConfigLoader.getServerProperty("domain"), SoileConfigLoader.getSessionProperty("cookiePath")))
								{
									if(current.name().equals(SoileConfigLoader.getSessionProperty("sessionCookieID")))
									{		
										//add the cookie to the other session.
										newCookieSession.cookieStore().put(current);
										foundSessionCookie = true;
									}
								}
								context.assertTrue(foundSessionCookie);
								String token = new JsonObject(authed.body().toString()).getString("token");
								context.assertTrue(token != null);
								Async cookieTest = context.async();
								Async tokenTest = context.async();
								Async failedTest = context.async();
								// test that cookie is working
								newCookieSession.post(port,"localhost","/test/auth").send().onComplete(authTest ->									
								{
									if(authTest.succeeded())
									{
										HttpResponse<Buffer> authWorked = authTest.result();
										try
										{												
											context.assertTrue(authWorked.bodyAsJsonObject().getBoolean("authenticated"));
											context.assertEquals("testUser", authWorked.bodyAsJsonObject().getString("user"));
											context.assertTrue(authWorked.bodyAsJsonObject().getJsonArray("roles").contains(Roles.Participant.toString()));
											Async loggedOut = context.async();
											testLogout(context, newCookieSession,401)
											.onComplete(logout -> {
												loggedOut.complete();
											});
										}
										catch(Exception e)
										{
											context.fail(e.getMessage());
										}
									}
									else
									{
										context.fail("Couldn't complete auth test request");
									}
									cookieTest.complete();											
								});
								// test that token is working
								newTokenSession.addHeader("Authorization", "Bearer "+ token );
								newTokenSession.post(port,"localhost","/test/auth").send().onComplete(authTest ->									
								{
									if(authTest.succeeded())
									{
										HttpResponse<Buffer> authWorked = authTest.result();
										try
										{
											context.assertTrue(new JsonObject(authWorked.body().toString()).getBoolean("authenticated"));
											context.assertEquals("testUser", new JsonObject(authWorked.body().toString()).getString("user"));
											context.assertTrue(authWorked.bodyAsJsonObject().getJsonArray("roles").contains(Roles.Participant.toString()));
											Async loggedOut = context.async();
											// Logout will have no effect if we supply a Token in the Header (which cannot be invalidated)
											testLogout(context, newTokenSession, 200)
											.onComplete(logout -> {
												loggedOut.complete();
											});
										}
										catch(Exception e)
										{
											context.fail(e.getMessage());
										}
									}
									else
									{
										context.fail("Couldn't complete auth test request");
									}
									tokenTest.complete();											
								});
								// test that no cookie and token fails.
								unregisteredSession.post(port,"localhost","/test/auth").send().onComplete(authTest ->									
								{
									if(authTest.succeeded())
									{
										HttpResponse<Buffer> authWorked = authTest.result();
										try
										{
											context.assertEquals(401,authWorked.statusCode());												
										}
										catch(Exception e)
										{
											context.fail(e.getMessage());
										}
									}
									else
									{
										context.fail("Couldn't complete auth test request");
									}
									failedTest.complete();											
								});
								Async sessionCookieTest = context.async();
								session.post(port,"localhost","/test/auth").send().onComplete(authTest ->									
								{
									if(authTest.succeeded())
									{
										HttpResponse<Buffer> authWorked = authTest.result();
										try
										{
											context.assertTrue(new JsonObject(authWorked.body().toString()).getBoolean("authenticated"));
											context.assertEquals("testUser", new JsonObject(authWorked.body().toString()).getString("user"));
											context.assertTrue(authWorked.bodyAsJsonObject().getJsonArray("roles").contains(Roles.Participant.toString()));
											Async loggedOut = context.async();
											testLogout(context, session,401)
											.onComplete(logout -> {
												loggedOut.complete();
											});
										}
										catch(Exception e)
										{
											context.fail(e.getMessage());
										}
									}
									else
									{
										context.fail("Couldn't complete auth test request");
									}
									sessionCookieTest.complete();											
								});
								login.complete();

							}).onFailure( fail -> {
								login.complete();
								context.fail(fail.getCause());
							});								

						}
						else
						{
							context.fail("Couldn't create user");								
						}
						userCreationAsync.complete();

					});
		}
		catch(Exception e)
		{
			context.fail(e.getMessage());
			userCreationAsync.complete();
		}
	}  


	private Future<Void> testLogout(TestContext context, WebClientSession session, int expectedResult)
	{
		Promise<Void> loggedOutPromise = Promise.promise(); 
		
		
		session.post(port,"localhost","/logout").send().onComplete(logOut -> {
			session.post(port,"localhost","/test/auth").send().onComplete(authTest ->
			{
				if(authTest.succeeded())
				{
					HttpResponse<Buffer> authWorked = authTest.result();
					try
					{
						context.assertEquals(expectedResult,authWorked.statusCode());												
					}
					catch(Exception e)
					{
						context.fail(e.getMessage());
					}
				}
				else
				{
					context.fail("Couldn't complete auth test request");
				}
				loggedOutPromise.complete();		
				
			});
		});
		return loggedOutPromise.future();
	}
}
