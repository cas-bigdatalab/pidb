package cn.pidb.engine

import java.io._
import java.util.UUID
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import cn.pidb.blob._
import cn.pidb.blob.storage.{BlobStorage, RollbackCommand}
import cn.pidb.engine.blob._
import cn.pidb.engine.blob.extensions.{GraphServiceContext, TransactionRecordStateAware}
import cn.pidb.engine.cypherplus.{CustomPropertyProvider, CypherPluginRegistry, ValueMatcher}
import cn.pidb.util.ConfigurationEx._
import cn.pidb.util.ReflectUtils._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{Configuration, Logging, Neo2JavaValueMapper, StreamUtils}
import org.apache.commons.io.IOUtils
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.neo4j.bolt.v1.messaging.Neo4jPack
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.driver.internal.packstream.PackStream
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.newapi.DefaultPropertyCursor
import org.neo4j.kernel.impl.proc.{Procedures, TypeMappers}
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyRecord}
import org.neo4j.kernel.impl.transaction.state.RecordAccess
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.values.storable.ValueWriter
import org.springframework.context.support.FileSystemXmlApplicationContext

import scala.collection.mutable

/**
  * Created by bluejoe on 2018/11/29.
  */
trait BlobPropertyStoreService {
  val customPropertyProvider: CustomPropertyProvider;
  val valueMatcher: ValueMatcher;
  val graphServiceContext: GraphServiceContext;
  val configuration: Configuration;
  val blobStorage: BlobStorage;
  val blobIdFactory: BlobIdFactory;
  val blobIO: BlobIO;
}

object BlobPropertyStoreService {
  def of(config: Config): BlobPropertyStoreService =
    config.asInstanceOf[GraphServiceContext].getBlobPropertyStoreService;

  def of(state: QueryState): BlobPropertyStoreService = of({
    if (state.query.isInstanceOf[UpdateCountingQueryContext])
      state._get("query.inner.inner.transactionalContext.tc.graph.graph.config")
    else
      state._get("query.inner.transactionalContext.tc.graph.graph.config")
  }.asInstanceOf[Config])

  def of(cursor: DefaultPropertyCursor): BlobPropertyStoreService =
    of(cursor._get("read.properties.configuration").asInstanceOf[Config]);

  def of(trsa: TransactionRecordStateAware): BlobPropertyStoreService =
    trsa.blobPropertyStoreService;

  def of(propertyRecords: RecordAccess[PropertyRecord, PrimitiveRecord]): BlobPropertyStoreService =
    of(propertyRecords._get("loader.val$store.configuration").asInstanceOf[Config]);

  def of(valueWriter: ValueWriter[_]): BlobPropertyStoreService =
    of(valueWriter._get("stringAllocator").asInstanceOf[TransactionRecordStateAware]);

  private def getFromCurrentThread(): BlobPropertyStoreService = of(ThreadVars.get[Config]("config"))

  def of(unpacker: Neo4jPack.Unpacker): BlobPropertyStoreService = getFromCurrentThread

  def of(packer: Neo4jPack.Packer): BlobPropertyStoreService = getFromCurrentThread

  def of(unpacker: PackStream.Unpacker): BlobPropertyStoreService = getFromCurrentThread

  def of(packer: PackStream.Packer): BlobPropertyStoreService = getFromCurrentThread
}

class BlobPropertyStoreServiceImpl(storeDir: File, conf: Config, proceduresService: Procedures)
  extends Lifecycle with BlobPropertyStoreService with Logging {
  override val blobIO: BlobIO = new BlobIO(this);
  override val graphServiceContext: GraphServiceContext = conf.asInstanceOf[GraphServiceContext];
  override val blobIdFactory = new BlobIdFactory {
    private def fromUUID(uuid: UUID): BlobId = new BlobId() {
      def asLongArray(): Array[Long] = {
        Array[Long](uuid.getMostSignificantBits, uuid.getLeastSignificantBits);
      }

      def asByteArray(): Array[Byte] = {
        StreamUtils.convertLongArray2ByteArray(asLongArray());
      }

      override def asLiteralString(): String = {
        uuid.toString;
      }
    }

    def fromLongArray(mostSigBits: Long, leastSigBits: Long) = fromUUID(new UUID(mostSigBits, leastSigBits));

    override def create(): BlobId = fromUUID(UUID.randomUUID());

    override def fromBytes(bytes: Array[Byte]): BlobId = {
      val is = new ByteArrayInputStream(bytes);
      fromLongArray(is.readLong(), is.readLong());
    }

    override def readFromStream(is: InputStream): BlobId = {
      fromBytes(is.readBytes(16))
    }

    override def fromLiteralString(bid: String): BlobId = {
      fromUUID(UUID.fromString(bid));
    }
  }

  override val configuration = new Configuration() {
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

  conf.asInstanceOf[GraphServiceContext].contextPut[BlobPropertyStoreService](this);

  val blobStorage: BlobStorage = configuration.getRaw("blob.storage")
    .map(Class.forName(_).newInstance().asInstanceOf[BlobStorage])
    .getOrElse(createDefaultBlobStorage);

  val _mapper = new Neo2JavaValueMapper(proceduresService.valueMapper().asInstanceOf[TypeMappers]);
  var _blobServer: HttpBlobServer = _;

  override val (valueMatcher, customPropertyProvider) = {
    val cypherPluginRegistry = configuration.getRaw("blob.plugins.conf").map(x => {
      val xml = new File(x);

      val path =
        if (xml.isAbsolute) {
          xml.getPath
        }
        else {
          val configFilePath = configuration.getRaw("config.file.path")
          if (configFilePath.isDefined) {
            new File(new File(configFilePath.get).getParentFile, x).getAbsoluteFile.getCanonicalPath
          }
          else {
            xml.getAbsoluteFile.getCanonicalPath
          }
        }

      logger.info(s"loading plugins: $path");
      val appctx = new FileSystemXmlApplicationContext("file:" + path);
      appctx.getBean[CypherPluginRegistry](classOf[CypherPluginRegistry]);
    }).getOrElse(new CypherPluginRegistry());

    (cypherPluginRegistry.createValueComparatorRegistry(configuration), cypherPluginRegistry.createCustomPropertyProvider(configuration));
  }

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
  }

  override def stop(): Unit = {
    if (_blobServer != null) {
      _blobServer.shutdown();
    }

    blobStorage.disconnect();
    logger.info(s"blob storage shutdown: ${blobStorage}");
  }

  private def startBlobServerIfNeeded(): Unit = {
    _blobServer = if (!conf.enabledBoltConnectors().isEmpty) {
      val httpPort = configuration.getValueAsInt("blob.http.port", 1224);
      val servletPath = configuration.getValueAsString("blob.http.servletPath", "/blob");
      val blobServer = new HttpBlobServer(httpPort, servletPath);
      //set url
      val hostName = configuration.getValueAsString("blob.http.host", "localhost");
      val httpUrl = s"http://$hostName:${httpPort}$servletPath";

      conf.asInstanceOf[GraphServiceContext].contextPut("blob.server.connector.url", httpUrl);
      blobServer.start();
      blobServer;
    }
    else {
      null;
    }
  }

  private def createDefaultBlobStorage() = new BlobStorage with Logging {
    var _blobDir: File = _;

    def saveBatch(blobs: Iterable[(BlobId, Blob)]) = {
      val files = blobs.map(x => {
        val (bid, blob) = x;
        val file = fileOfBlob(bid);
        file.getParentFile.mkdirs();

        val fos = new FileOutputStream(file);
        fos.write(bid.asByteArray());
        fos.writeLong(blob.mimeType.code);
        fos.writeLong(blob.length);

        blob.offerStream { bis =>
          IOUtils.copy(bis, fos);
        }
        fos.close();

        file;
      }
      )

      new RollbackCommand() {
        override def perform(): Unit = {
          files.foreach(_.delete());
        }
      }
    }

    def loadBatch(ids: Iterable[BlobId]): Iterable[Option[Blob]] = {
      ids.map(id => Some(readFromBlobFile(fileOfBlob(id))._2));
    }

    def deleteBatch(ids: Iterable[BlobId]) = {
      ids.foreach(id => fileOfBlob(id).delete());

      new RollbackCommand() {
        override def perform(): Unit = {
          //TODO: create files?
        }
      }
    }

    private def fileOfBlob(bid: BlobId): File = {
      val idname = bid.asLiteralString();
      new File(_blobDir, s"${idname.substring(32, 36)}/$idname");
    }

    private def readFromBlobFile(blobFile: File): (BlobId, Blob) = {
      val fis = new FileInputStream(blobFile);
      val blobId = blobIdFactory.readFromStream(fis);
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

    override def initialize(storeDir: File, blobIdFactory: BlobIdFactory, conf: Configuration): Unit = {
      val baseDir: File = storeDir; //new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
      _blobDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob"));
      _blobDir.mkdirs();
      logger.info(s"using storage dir: ${_blobDir.getCanonicalPath}");
    }

    override def disconnect(): Unit = {
    }
  }

  override def start(): Unit = {
    blobStorage.initialize(storeDir, blobIdFactory, configuration);
    logger.info(s"instant blob storage initialized: ${blobStorage}");

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