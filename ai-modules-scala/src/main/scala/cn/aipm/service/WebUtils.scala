package cn.aipm.service

import java.io.{File, InputStream}

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpStatus}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject

object WebUtils {

  def doGet(reqUrl:String, contents:Map[String,Any]): String ={
    var resStr = ""

    val client = HttpClients.createDefault()
    val httpGet: HttpGet = new HttpGet(reqUrl)
    //设置提交参数为application/json
    //    post.addHeader("Content-Type", "multipart/form-data")


    //执行请求
    val response = client.execute(httpGet)
    //返回结果
    //    val allHeaders: Array[Header] = post.getAllHeaders
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode == HttpStatus.SC_OK) {
      val resEntity:HttpEntity = response.getEntity
      resStr = EntityUtils.toString(resEntity,"UTF-8")
    }
    resStr
  }


  def doJsonPost(reqUrl:String, contents:Map[String,String]): String ={
    var resStr = ""
    val jsonObj :JSONObject = new JSONObject(contents)
    val client = HttpClients.createDefault()
    val post: HttpPost = new HttpPost(reqUrl)
    //设置提交参数为application/json
    //    post.addHeader("Content-Type", "multipart/form-data")
    post.addHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(jsonObj.toString))
    //执行请求
    val response = client.execute(post)
    //返回结果
    //    val allHeaders: Array[Header] = post.getAllHeaders
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode == HttpStatus.SC_OK) {
      val resEntity:HttpEntity = response.getEntity
      resStr = EntityUtils.toString(resEntity,"UTF-8")
    }
    response.close()
    resStr
  }


  def doPost(reqUrl: String, strContents:Map[String,String]=Map(),fileContents:Map[String,File]=Map(),
             inStreamContents:Map[String,InputStream]=Map()): String = {
    var resStr = ""
    try {
      val httpClient = HttpClients.createDefault()
      val httpPost = new HttpPost(reqUrl)
      val mEntityBuilder = MultipartEntityBuilder.create()
      mEntityBuilder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
      for ((key , value) <- strContents){
        mEntityBuilder.addTextBody(key,value)
      }
      for ((key , value) <- fileContents){
        mEntityBuilder.addBinaryBody(key,value)
      }
      for ((key , value) <- inStreamContents){
        // 使用此方式request-body中无数据 TODO:后续查找问题根源
        // val inputStreamBody = new InputStreamBody(value, ContentType.APPLICATION_OCTET_STREAM)
        // mEntityBuilder.addPart(key, inputStreamBody)
        mEntityBuilder.addBinaryBody(key,_inputStreamToByteArray(value),ContentType.DEFAULT_BINARY,"tmp.bin")
      }

      httpPost.setEntity(mEntityBuilder.build())

      val response = httpClient.execute(httpPost)
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode == HttpStatus.SC_OK) {
        val resEntity:HttpEntity = response.getEntity
        resStr = EntityUtils.toString(resEntity,"UTF-8")
      }
      response.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    resStr
  }

  private def _inputStreamToByteArray(is: InputStream): Array[Byte] = {
    val buf = ListBuffer[Byte]()
    var b = is.read()
    while (b != -1) {
      buf.append(b.byteValue)
      b = is.read()
    }
    buf.toArray
  }

}
