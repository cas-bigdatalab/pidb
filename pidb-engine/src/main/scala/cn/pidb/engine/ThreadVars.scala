package cn.pidb.engine

import scala.collection.mutable

/**
  * Created by bluejoe on 2018/11/28.
  */
//TODO: remove data while thread destroyed
object ThreadVars {
  val _vars = mutable.Map[Thread, mutable.Map[String, Any]]();

  def put(name: String, value: Any, thread: Thread = Thread.currentThread()) = {
    _vars.getOrElseUpdate(thread, mutable.Map[String, Any]())(name) = value;
  }

  def get[T](name: String, thread: Thread = Thread.currentThread()) = {
    _vars(thread)(name).asInstanceOf[T];
  }
}
