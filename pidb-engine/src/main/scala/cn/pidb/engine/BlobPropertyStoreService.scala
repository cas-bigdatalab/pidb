package cn.pidb.engine

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import cn.pidb.blob._
import cn.pidb.blob.storage.BlobStorage
import cn.pidb.engine.blob._
import cn.pidb.util.ConfigEx._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{Config, Logging, Neo2JavaValueMapper}
import org.apache.commons.io.IOUtils
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.neo4j.kernel.impl.proc.{Procedures, TypeMappers}
import org.neo4j.kernel.lifecycle.Lifecycle
import org.springframework.context.support.FileSystemXmlApplicationContext

import scala.collection.mutable

/**
  * Created by bluejoe on 2018/11/29.
  */
class BlobPropertyStoreService(storeDir: File, conf: org.neo4j.kernel.configuration.Config, proceduresService: Procedures) extends Lifecycle with Logging {

  val config = new Config() {
    override def getRaw(name: String): Option[String] = {
      val raw = conf.getRaw(name);
      if (raw.isPresent) {
        Some(raw.get())
      }
      else {
        None
      }
    }
  }

  conf.asInstanceOf[RuntimeContextHolder].putRuntimeContext[BlobPropertyStoreService](this);

  val _cypherPluginRegistry: CypherPluginRegistry = config.getRaw("blob.plugins.conf").map(x => {
    val path = new File(x).getAbsolutePath;
    logger.info(s"loading plugins: $path");
    val appctx = new FileSystemXmlApplicationContext("file://" + path);
    appctx.getBean[CypherPluginRegistry](classOf[CypherPluginRegistry]);
  }).getOrElse(new CypherPluginRegistry())

  var _instantStorage: BlobStorage = _;
  val _mapper = new Neo2JavaValueMapper(proceduresService.valueMapper().asInstanceOf[TypeMappers]);
  var _blobServer: HttpBlobServer = _;

  val _valueMatcher = new ValueComparatorRegistry(_cypherPluginRegistry.getComparators());
  val _propertyProvider: CustomPropertyProvider = new CustomPropertyExtractorRegistry(_cypherPluginRegistry.getExtractors)

  def getCustomPropertyProvider: CustomPropertyProvider = _propertyProvider;

  def getValueMatcher: ValueMatcher = _valueMatcher;

  def getBlobStorage: BlobStorage = _instantStorage;

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
  }

  override def stop(): Unit = {
    if (_blobServer != null) {
      _blobServer.shutdown();
    }

    _instantStorage.disconnect();
    logger.info(s"blob storage shutdown: ${_instantStorage}");
  }

  private def startBlobServerIfNeeded(): Unit = {
    _blobServer = if (!conf.enabledBoltConnectors().isEmpty) {
      val httpPort = config.getValueAsInt("blob.http.port", 1224);
      val servletPath = config.getValueAsString("blob.http.servletPath", "/blob");
      val blobServer = new HttpBlobServer(httpPort, servletPath);
      //set url
      val hostName = config.getValueAsString("blob.http.host", "localhost");
      val httpUrl = s"http://$hostName:${httpPort}$servletPath";

      conf.asInstanceOf[RuntimeContextHolder].putRuntimeContext("blob.server.connector.url", httpUrl);
      blobServer.start();
      blobServer;
    }
    else {
      null;
    }
  }

  val DEFAULT_BLOB_STORAGE = new BlobStorage with Logging {
    var _blobDir: File = _;
    var _blobIdFactory: BlobIdFactory = _;

    override def save(bid: BlobId, blob: Blob) = {
      val file = blobFile(bid);
      file.getParentFile.mkdirs();

      val fos = new FileOutputStream(file);
      fos.write(bid.asByteArray());
      fos.writeLong(blob.mimeType.code);
      fos.writeLong(blob.length);

      blob.offerStream { bis =>
        IOUtils.copy(bis, fos);
      }
      fos.close();
    }

    override def load(bid: BlobId): InputStreamSource = {
      readFromBlobFile(blobFile(bid))._2.streamSource
    }

    override def delete(bid: BlobId) = {
      blobFile(bid).delete()
    }

    private def blobFile(bid: BlobId): File = {
      val idname = bid.asLiteralString();
      new File(_blobDir, s"${idname.substring(32, 36)}/$idname");
    }

    private def readFromBlobFile(blobFile: File): (BlobId, Blob) = {
      val fis = new FileInputStream(blobFile);
      val blobId = _blobIdFactory.readFromStream(fis);
      val mimeType = MimeType.fromCode(fis.readLong());
      val length = fis.readLong();
      fis.close();

      val blob = Blob.fromInputStreamSource(new InputStreamSource() {
        def offerStream[T](consume: (InputStream) => T): T = {
          val is = new FileInputStream(blobFile);
          //NOTE: skip
          is.skip(8 * 4);
          val t = consume(is);
          is.close();
          t;
        }
      }, length, Some(mimeType));

      (blobId, blob);
    }

    override def initialize(storeDir: File, blobIdFactory: BlobIdFactory, conf: Config): Unit = {
      val baseDir: File = storeDir; //new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
      _blobDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob"));
      _blobIdFactory = blobIdFactory;
      _blobDir.mkdirs();
      logger.info(s"using storage dir: ${_blobDir.getCanonicalPath}");
    }

    override def disconnect(): Unit = {
    }
  }

  override def start(): Unit = {
    this._instantStorage = config.getRaw("blob.storage")
      .map(Class.forName(_).newInstance().asInstanceOf[BlobStorage])
      .getOrElse(DEFAULT_BLOB_STORAGE);

    this._instantStorage.initialize(storeDir, BlobIO.blobIdFactory, config);
    logger.info(s"instant blob storage initialized: ${_instantStorage}");

    //use getRuntimeContext[BlobPropertyStoreService]
    //config.asInstanceOf[RuntimeContextHolder].putRuntimeContext[InstantBlobStorage](_instantStorage);

    registerProcedure(classOf[DefaultBlobFunctions]);
    startBlobServerIfNeeded();
  }

  private def registerProcedure(procedures: Class[_]*) {
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }
}

//TODO: clear cache while session closed
object BlobCacheInSession extends Logging {
  val cache = mutable.Map[String, Blob]();

  def put(key: BlobId, blob: Blob): Unit = {
    val s = key.asLiteralString();
    cache(s) = blob;
    logger.debug(s"BlobCacheInSession: $s");
  }

  def invalidate(key: String) = {
    cache.remove(key);
  }

  def get(key: BlobId): Option[Blob] = cache.get(key.asLiteralString());

  def get(key: String): Option[Blob] = cache.get(key);
}

class HttpBlobServer(httpPort: Int, servletPath: String) extends Logging {
  var _server: Server = _;

  def start(): Unit = {
    _server = new Server(httpPort);
    val blobStreamServlet = new BlobStreamServlet();
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    _server.setHandler(context);
    //add servlet
    context.addServlet(new ServletHolder(blobStreamServlet), servletPath);
    _server.start();

    logger.info(s"blob server started on http://localhost:$httpPort$servletPath");
  }

  def shutdown(): Unit = {
    _server.stop();
  }

  class BlobStreamServlet extends HttpServlet {
    override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      val blobId = req.getParameter("bid");
      val opt = BlobCacheInSession.get(blobId);
      if (opt.isDefined) {
        resp.setContentType(opt.get.mimeType.text);
        resp.setContentLength(opt.get.length.toInt);
        opt.get.offerStream(IOUtils.copy(_, resp.getOutputStream));
      }
      else {
        resp.sendError(500, s"invalid blob id: $blobId");
      }
    }
  }

}