package fi.abo.kogni.soile2.http_server.requestHandling;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fi.abo.kogni.soile2.datamanagement.datalake.DataLakeResourceManager;
import fi.abo.kogni.soile2.datamanagement.git.GitFile;
import fi.abo.kogni.soile2.http_server.routes.SoileRouter;
import fi.abo.kogni.soile2.projecthandling.projectElements.Element;
import io.vertx.core.MultiMap;
import io.vertx.core.file.FileProps;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.impl.Utils;
/**
 * A File Provider that uses IDs for retrieval
 * @author Thomas Pfau
 *
 */
public class IDSpecificFileProvider {

	private static final Logger LOGGER = LogManager.getLogger(IDSpecificFileProvider.class);
	DataLakeResourceManager mgr;
	SizedCache<GitFile, FileWithProps> cache = new SizedCache<GitFile, FileWithProps>(1000);

	/**
	 * Default constructor
	 * @param mgr the {@link DataLakeResourceManager} to use for retrieval
	 */
	public IDSpecificFileProvider(DataLakeResourceManager mgr)
	{
		this.mgr = mgr;
	}
	
	/**
	 * Handle a context returning the requested file. The Element provided indicates what repository 
	 * needs to be used.
	 * @param context the Routingcontext to handle
	 * @param resourceType The type of resource requested.
	 */
	public void handleContext(RoutingContext context, Element resourceType)
	{
		String id = context.pathParam("id");
		String version = context.pathParam("version");
		String fileName = SoileRouter.normalizePath(context.pathParam("*"));
		// We only handle files.
		if(fileName.endsWith("/"))
		{
			context.next();
			return;
		}
		// need to do this translation (unfortunately)
		GitFile target = new GitFile(fileName, resourceType.getTypeID() + id, version);		
		returnResource(context, target);
	}

	/**
	 * Send the data associated with the given GitFile as response to the contexts requests (if available)
	 * @param context the context to respond to 
	 * @param requestTarget the file that's the target of the request
	 */
	public void returnResource(RoutingContext context, GitFile requestTarget)
	{
		LOGGER.debug(requestTarget.toJson());
		FileWithProps localFile = cache.get(requestTarget);
		if(localFile != null)
		{
			sendFile(context,localFile);
		}
		else
		{			
			mgr.getElement(requestTarget)
			.onSuccess(file -> {
				context.vertx().fileSystem().props(file.getAbsolutePath())
				.onSuccess(props -> {
					FileWithProps current = new FileWithProps(props, file.getAbsolutePath(), file.getFormat());
					cache.put(requestTarget, current);
					sendFile(context,current);
				})
				.onFailure(err -> handleNonFatalError(context, err, requestTarget));
			})
			.onFailure(err -> handleNonFatalError(context, err, requestTarget));
		}
	}

	private void handleNonFatalError(RoutingContext context, Throwable err, GitFile requestTarget)
	{
		LOGGER.error("Problem finding file: " + requestTarget.toString());
		LOGGER.error(err);
		context.next();
	}


	private void sendFile(RoutingContext context, FileWithProps file)
	{
		final HttpServerResponse response = context.response();

		writeCacheHeaders(context.request(), file.getProps());
		if (context.request().method() == HttpMethod.HEAD) {
			response.end();
		}
		String contentType = file.getMimeType();
		if (contentType.startsWith("text")) {
			response.putHeader(HttpHeaders.CONTENT_TYPE, contentType + ";charset=" + Charset.defaultCharset().name());
		} else {
			response.putHeader(HttpHeaders.CONTENT_TYPE, contentType);
		}
		response.sendFile(file.getPath()).andThen(res2 -> {
			if (res2.failed()) {
				if (!context.request().isEnded()) {
					context.request().resume();
				}
				context.fail(res2.cause());
			}
		});
	}

	private void writeCacheHeaders(HttpServerRequest request, FileProps props) {

		MultiMap headers = request.response().headers();

		// We use cache-control and last-modified
		// We *do not use* etags and expires (since they do the same thing - redundant)
		Utils.addToMapIfAbsent(headers, HttpHeaders.CACHE_CONTROL, "public, immutable, max-age=" + StaticHandler.DEFAULT_MAX_AGE_SECONDS);
		Utils.addToMapIfAbsent(headers, HttpHeaders.LAST_MODIFIED, Utils.formatRFC1123DateTime(props.lastModifiedTime()));
		// We send the vary header (for intermediate caches)
		// (assumes that most will turn on compression when using static handler)
		if (request.headers().contains(HttpHeaders.ACCEPT_ENCODING)) {
			Utils.addToMapIfAbsent(headers, HttpHeaders.VARY, "accept-encoding");		      
		}
		// date header is mandatory
		headers.set("date", Utils.formatRFC1123DateTime(System.currentTimeMillis()));
	}

	private class FileWithProps
	{
		FileProps props;
		String path;
		String mimeType;
		public FileProps getProps() {
			return props;
		}
		public String getPath() {
			return path;
		}
		public String getMimeType() {
			return mimeType;
		}
		public FileWithProps(FileProps props, String path, String mimeType) {
			super();
			this.props = props;
			this.path = path;
			this.mimeType = mimeType;
		}

	}

	private class SizedCache<K, V> extends LinkedHashMap<K, V>
	{
		private static final long serialVersionUID = 1L;
		private int maxSize;

		public SizedCache(int maxSize)
		{
			super();
			this.maxSize = maxSize; 
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
			return size() > maxSize;
		}
	}


}
