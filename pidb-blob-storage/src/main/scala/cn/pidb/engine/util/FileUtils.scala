package cn.pidb.engine.util

import java.io.{File, FileInputStream, InputStream}

import cn.pidb.blob.{Blob, BlobId, InputStreamSource, MimeType}
import cn.pidb.engine.refactor.buffer.exception.FilePathIsNotDirectoryException
import cn.pidb.util.StreamUtils._

import scala.collection.mutable

object FileUtils {

  def blobFile(bid: BlobId, blobDir : File): File = {
    val idname = bid.asLiteralString()
    new File(blobDir, s"${idname.substring(32, 36)}/$idname")
  }

  def readFromBlobFile(blobFile: File): (BlobId, Blob) = {
    val fis = new FileInputStream(blobFile)
    val blobId = BlobId.fromLongArray(fis.readLong(), fis.readLong())
    val mimeType = MimeType.fromCode(fis.readLong())
    val length = fis.readLong()
    fis.close()

    val blob = Blob.fromInputStreamSource(new InputStreamSource() {
      def offerStream[T](consume: InputStream => T): T = {
        val is = new FileInputStream(blobFile)
        //NOTE: skip
        is.skip(8 * 4)
        val t = consume(is)
        is.close()
        t
      }
    }, length, Some(mimeType))

    (blobId, blob)
  }

  def listAllFiles (directory : File) : List[File] = {
    if (directory.isFile) throw new FilePathIsNotDirectoryException(s"the input $directory is not a directory")
    ls(directory, new mutable.HashSet[File]()).toList
  }

  def ls(file: File, resultFileName: mutable.HashSet[File]): mutable.HashSet[File] = {
    val files = file.listFiles
    if (files == null) return resultFileName // 判断目录下是不是空的
    for (f <- files) {
      if (f.isDirectory) { // 判断是否文件夹
        resultFileName.add(f)
        ls(f, resultFileName) // 调用自身,查找子目录
      }
      else resultFileName.add(f)
    }
    resultFileName
  }
}
