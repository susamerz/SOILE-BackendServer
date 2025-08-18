package fi.abo.kogni.soile2.http_server.verticles;


import java.util.LinkedList;
import java.util.List;

import fi.abo.kogni.soile2.http_server.auth.SoileAuthorization.TargetElementType;
import fi.abo.kogni.soile2.utils.SoileCommUtils;
import fi.abo.kogni.soile2.utils.SoileConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

/**
 * Permission handling verticle
 * @author Thomas Pfau
 *
 */
public class PermissionVerticle extends AbstractVerticle {

	MongoClient client;	
	private List<MessageConsumer<JsonObject>> consumers;
	/**
	 * Default constructor
	 */
	public PermissionVerticle() {
		super();		
	}
	
	@Override
	public void start(Promise<Void> startPromise) throws Exception {
			client = MongoClient.createShared(vertx, SoileConfigLoader.getMongoCfg(), "PERMISSION_VERTICLE_MONGO");
			consumers = new LinkedList<>();
			consumers.add(vertx.eventBus().consumer("soile.permissions.checkTargets", this::checkPermissions));
			startPromise.complete();
	}

	@Override
	public void stop(Promise<Void> stopPromise)
	{
		@SuppressWarnings("rawtypes")
		List<Future<Void>> undeploymentFutures = new LinkedList<Future<Void>>();
		for(MessageConsumer<JsonObject> consumer : consumers)
		{
			undeploymentFutures.add(consumer.unregister());
		}				
		Future.all(undeploymentFutures).mapEmpty().
		onSuccess(v -> {
			stopPromise.complete();
		})
		.onFailure(err -> stopPromise.fail(err));			
	}
	
	private void checkPermissions(Message<JsonObject> permissionMessage)
	{
		try {
			JsonObject request = permissionMessage.body();		
			String targetCollection = SoileConfigLoader.getDataBaseforElement(TargetElementType.valueOf(request.getString("elementType")));
			JsonArray permissions = request.getJsonArray("permissionSettings");
			JsonArray targets = new JsonArray();
			for(int i = 0; i < permissions.size(); ++i)
			{
				targets.add(permissions.getJsonObject(i).getString("target"));			
			}
			client.findWithOptions(targetCollection, new JsonObject().put("_id", new JsonObject().put("$in", targets)), new FindOptions().setFields(new JsonObject().put("_id", 1)))
			.onSuccess(res -> {
				for(int i = 0; i < res.size(); ++i)
				{
					targets.remove(res.get(i).getString("_id"));
				}
				if(targets.size() != 0)
				{
					// two options: 1. targets had non unique entries, which is not allowed (then targets.
					// 				2. one or more items does not exist.
					// TODO: create a more helpful message.
					permissionMessage.fail(400, "Invalid or duplicate permissions in requested changes");
				}
				else
				{
					permissionMessage.reply(SoileCommUtils.successObject());
				}
			})
			.onFailure(err -> permissionMessage.fail(500, err.getMessage()));
		}
		catch(Exception err)
		{
			permissionMessage.fail(400, err.getMessage());
		}
	}
}
