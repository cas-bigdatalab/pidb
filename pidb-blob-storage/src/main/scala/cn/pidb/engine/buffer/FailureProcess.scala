package cn.pidb.engine.buffer

import cn.pidb.engine.storage.Storage
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
    f.deleteBatch(rollBackLog.map(f.getIdFac.fromLiteralString).toList)
    rollBackLog.clean()
  }
}