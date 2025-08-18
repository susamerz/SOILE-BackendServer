package fi.abo.kogni.soile2.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;

import io.vertx.core.Future;
import io.vertx.ext.web.FileUpload;

public class DataProvider {
	public static String createTempDataDirectory(String DirToCopy) throws IOException
	{
		String tmpDir = Files.createTempDirectory("SoileDataDirectory").toString();
		FileUtils.copyDirectory(new File(DirToCopy), new File(tmpDir));
		return tmpDir;
	}
	
	public static FileUpload getFileUploadForTarget(String localFile, String sourceFileName, String contentType)
	{
		return new MockFileUpload(localFile, sourceFileName, contentType);
		};
	}
	
	class MockFileUpload implements FileUpload
	{

		public MockFileUpload(String localFile, String sourceFileName, String contentType) {
			super();
			this.localFile = localFile;
			this.sourceFileName = sourceFileName;
			this.contentType = contentType;
		}

		String localFile;
		String sourceFileName;
		String contentType;
		@Override
		public String name() {
			return localFile;
		}

		@Override
		public String uploadedFileName() {
			return localFile;
		}

		@Override
		public String fileName() {
			return sourceFileName;
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
			// TODO Auto-generated method stub
			return Future.succeededFuture();
		}
}
