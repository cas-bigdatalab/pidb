package cn.aipm.test
import java.io.File
import org.junit.{Assert, Test}

import cn.aipm.image.FaceInPhotoComparator
import cn.pidb.blob.Blob


class FaceInPhotoComparatorTest extends TestBase {
  val comparator = new FaceInPhotoComparator()
  comparator.initialize(config)
  @Test
  def test1():Unit={
    var imagePath1 = "E:/[face]/test2.jpg"
    var imagePath2 = "E:/[face]/test1.jpg"
    val res = comparator.compare(Blob.fromFile(new File(imagePath2)),Blob.fromFile(new File(imagePath1)))
    print(res)
  }

  @Test
  def test2():Unit={
    var imagePath1 = "E:/[face]/xy.jpg"
    var imagePath2 = "E:/[face]/test1.jpg"
    val res = comparator.compare(Blob.fromFile(new File(imagePath2)),Blob.fromFile(new File(imagePath1)))
    print(res)

  }

  @Test
  def test3():Unit={
    var imagePath1 = "E:/[face]/test2-1.jpg"
    var imagePath2 = "E:/[face]/test2-2.jpg"
    val res = comparator.compare(Blob.fromFile(new File(imagePath1)),Blob.fromFile(new File(imagePath2)))
    print(res)

  }


}
