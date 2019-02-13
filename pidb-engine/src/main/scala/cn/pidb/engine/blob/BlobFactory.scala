package cn.pidb.engine.blob

import java.io._
import java.net.{HttpURLConnection, URL}

import cn.pidb.blob._
import cn.pidb.util.Logging

class InlineBlob(bytes: Array[Byte], val length: Long, val mimeType: MimeType)
  extends Blob with Logging {

  override val streamSource: InputStreamSource = new InputStreamSource() {
    override def offerStream[T](consume: (InputStream) => T): T = {
      val fis = new ByteArrayInputStream(bytes);
      if (logger.isDebugEnabled)
        logger.debug(s"InlineBlob: length=${bytes.length}");
      val t = consume(fis);
      fis.close();
      t;
    }
  };
}

class RemoteBlob(urlConnector: String, blobId: BlobId, val length: Long, val mimeType: MimeType)
  extends Blob with Logging {

  override val streamSource: InputStreamSource = new InputStreamSource() {
    def offerStream[T](consume: (InputStream) => T): T = {
      val url = new URL(s"$urlConnector?bid=${blobId.asLiteralString()}");
      if (logger.isDebugEnabled)
        logger.debug(s"RemoteBlobValue: url=$url");
      val connection = url.openConnection().asInstanceOf[HttpURLConnection];
      connection.setDoOutput(false);
      connection.setDoInput(true);
      connection.setRequestMethod("GET");
      connection.setUseCaches(true);
      connection.setInstanceFollowRedirects(true);
      connection.setConnectTimeout(3000);
      connection.connect();
      val is = connection.getInputStream;
      val t = consume(is);
      is.close();
      t;
    }
  }
}