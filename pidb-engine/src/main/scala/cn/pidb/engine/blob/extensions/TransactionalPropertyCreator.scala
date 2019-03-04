package cn.pidb.engine.blob.extensions

import cn.pidb.engine.BlobPropertyStoreService
import cn.pidb.util.ReflectUtils._
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.store.DynamicRecordAllocator
import org.neo4j.kernel.impl.store.id.IdSequence
import org.neo4j.kernel.impl.store.record.DynamicRecord
import org.neo4j.kernel.impl.transaction.state.{PropertyCreator, PropertyTraverser, TransactionRecordState}

/**
  * Created by bluejoe on 2019/3/3.
  */
class TransactionalPropertyCreator(val recordState: TransactionRecordState, val raw: PropertyCreator)
  extends PropertyCreator(
    new TransactionalDynamicRecordAllocator(recordState, raw._get("stringRecordAllocator").asInstanceOf[DynamicRecordAllocator]),
    new TransactionalDynamicRecordAllocator(recordState, raw._get("arrayRecordAllocator").asInstanceOf[DynamicRecordAllocator]),
    raw._get("propertyRecordIdGenerator").asInstanceOf[IdSequence],
    raw._get("traverser").asInstanceOf[PropertyTraverser],
    raw._get("allowStorePointsAndTemporal").asInstanceOf[Boolean]
  ) with TransactionRecordStateAware {

}

class TransactionalDynamicRecordAllocator(val recordState: TransactionRecordState, val raw: DynamicRecordAllocator)
  extends DynamicRecordAllocator with TransactionRecordStateAware {
  override def nextRecord(): DynamicRecord = raw.nextRecord

  override def getRecordDataSize: Int = raw.getRecordDataSize
}

trait TransactionRecordStateAware {
  val recordState: TransactionRecordState;

  lazy val config = recordState._get("neoStores.config").asInstanceOf[Config]

  lazy val blobStorage = config.asInstanceOf[RuntimeContextHolder].getRuntimeContext[BlobPropertyStoreService]().getBlobStorage;
}