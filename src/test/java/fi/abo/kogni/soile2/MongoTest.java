package fi.abo.kogni.soile2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.embed.process.io.ProcessOutput;
import de.flapdoodle.embed.process.runtime.Network;
import de.flapdoodle.reverse.TransitionWalker.ReachedState;
import de.flapdoodle.reverse.transitions.Start;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public abstract class MongoTest extends SoileBaseTest {
	
	
	static Mongod MONGO;
	static ReachedState<RunningMongodProcess> state;	
	public MongoClient mongo_client;
		
	@BeforeClass
	public static void initialize() throws IOException {
		java.util.logging.Logger mongologger = java.util.logging.Logger.getLogger("org.mongodb.driver");
		mongologger.setLevel(Level.SEVERE);
		JsonObject config = new JsonObject(Files.readString(Paths.get(MongoTest.class.getClassLoader().getResource("soile_config.json").getPath())));
		
		Net net = Net.of(Network.getLocalHost().getHostAddress(), config.getJsonObject("mongo").getInteger("port"), Network.localhostIsIPv6());				
		MONGO = Mongod.builder()				
				.net(Start.to(Net.class).initializedWith(net))
				.processOutput(Start.to(ProcessOutput.class).initializedWith(ProcessOutput.silent()).withTransitionLabel("no output"))
				.build();		
		state = MONGO.start(Version.Main.V5_0);		
	}	

	@Override
	public void runBeforeTests(TestContext context)
	{	
		SoileConfigLoader.getMongoCfg().remove("username");
		SoileConfigLoader.getMongoCfg().remove("password");
		mongo_client = MongoClient.createShared(vertx, SoileConfigLoader.getMongoCfg());
	}
	
	@After
	@Override
	@SuppressWarnings("rawtypes")
	public void finishUp(TestContext context)
	{		
		final Async oasync = context.async();
		mongo_client.getCollections().andThen(cols ->{
			if(cols.succeeded())
			{
				Async shutDownVertxAsync = context.async();				
				HashMap<String, Async> asyncMap = new HashMap<>();
				List<Future<Void>> collectionsDropped = new LinkedList<Future<Void>>();
				for(String col : cols.result())
				{
					final Async async = context.async();
					
					asyncMap.put(col, async);					
				}			
				for(String col : cols.result())
				{
					collectionsDropped.add(mongo_client.dropCollection(col).onComplete(res ->				
					{
						asyncMap.get(col).complete();						
					}));					
				}
				if(collectionsDropped.size() == 0)
				{
					// either there was nothing to drop, then we can shut down the vertx instance
					//"No collections found. Proceeding"
					super.finishUp(context);
					shutDownVertxAsync.complete();
				}
				else
				{
					Future.all(collectionsDropped)
					.onSuccess(dropped -> {
						// or there were collections to be dropped, then we have to wait till those are dropped before shutting down vertx. 
						super.finishUp(context);
						shutDownVertxAsync.complete();
					})
					.onFailure(err -> context.fail(err));
				}
			}
			else
			{
				cols.cause().printStackTrace(System.out);
			}
			oasync.complete();
		});		
	}	
	
	@AfterClass
	public static void shutdown() {
		//System.out.println("Shutting down Mongo");
		state.current().stop();
		
	}

}
