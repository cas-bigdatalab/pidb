package cn.pidb.engine.blob.extensions

import cn.pidb.engine.{BlobPropertyStoreService}
import cn.pidb.util.ReflectUtils._
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.store.DynamicRecordAllocator
import org.neo4j.kernel.impl.store.id.IdSequence
import org.neo4j.kernel.impl.store.record.DynamicRecord
import org.neo4j.kernel.impl.transaction.state.{PropertyDeleter, PropertyCreator, PropertyTraverser, TransactionRecordState}

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

class TransactionalPropertyDeleter(val recordState: TransactionRecordState, val raw: PropertyDeleter)
  extends PropertyDeleter(raw._get("traverser").asInstanceOf[PropertyTraverser]) with TransactionRecordStateAware{
}

trait TransactionRecordStateAware {
  val recordState: TransactionRecordState;

  lazy val config = recordState._get("nodeStore.configuration").asInstanceOf[Config]

  lazy val blobPropertyStoreService: BlobPropertyStoreService = config.asInstanceOf[RuntimeContext].getBlobPropertyStoreService;
}