package fi.abo.kogni.soile2.projecthandling.utils;

import io.vertx.core.Future;
import io.vertx.ext.web.FileUpload;

/**
 * A helper class to generate a simple file upload for unit testing
 * @author Thomas Pfau
 *
 */
public class SimpleFileUpload implements FileUpload	
{
	
	private String uploadFileName;
	private String originalFileName;
	private String contentType;
	/**
	 * Default constructor
	 * @param uploadedFileName name of the uploaded file
	 * @param originalFileName name of the origial file
	 * @param contentType type of the content
	 */
	public SimpleFileUpload(String uploadedFileName, String originalFileName, String contentType)
	{	
		this.uploadFileName = uploadedFileName;
		this.originalFileName = originalFileName;
		this.contentType = contentType;
		
	}
	@Override
	public String name() {
		return null;
	}

	@Override
	public String uploadedFileName() {			
		return uploadFileName;
	}

	@Override
	public String fileName() {
		return originalFileName;
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public String contentType() {
		return contentType;
	}

	@Override
	public String contentTransferEncoding() {
		return null;
	}

	@Override
	public String charSet() {
		return null;
	}

	@Override
	public boolean cancel() {
		return false;
	}
	@Override
	public Future<Void> delete() {		
		return Future.succeededFuture();
	}
	
}