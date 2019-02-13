package cn.pidb.engine.refactor.buffer

import cn.pidb.engine.BlobIdFactory
import cn.pidb.engine.refactor.storage.Storage
trait FailureProcess {}
trait NotInParallel extends FailureProcess {

}
trait InParallel extends FailureProcess {}

/**
  * Roll back is a simple restore plan
  * this will restore the Buffer and Buffer's storage to the status before Transaction
  */
trait RollBack extends NotInParallel {
  def RollBack(f : Storage, rollBackLog : RollBackLogReader) : Unit = {
    f.deleteBatch(rollBackLog.map(BlobIdFactory.fromString).toList)
    rollBackLog.clean()
  }
}