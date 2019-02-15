
import java.io.File
import org.junit.{Assert, Test}

import cn.aipm.image.FaceSimilarityComparator
import cn.pidb.blob.Blob


class FaceSimilarityComparatorTest {

  @Test
  def test1():Unit={
    var imagePath1 = "E:/[face]/unknown/test.jpg"
    var imagePath2 = "E:/[face]/unknown/test2.jpg"
    val simComparator = new FaceSimilarityComparator()
    val res = simComparator.compare(Blob.fromFile(new File(imagePath1)),Blob.fromFile(new File(imagePath2)))
    print(res)
  }

  @Test
  def test2():Unit={
    var imagePath1 = "E:\\[pidb-ai-code]\\plate_number\\test1.jpg"
    var imagePath2 = "E:\\[pidb-ai-code]\\plate_number\\test2.jpg"
    val simComparator = new FaceSimilarityComparator()
    val res = simComparator.compare(Blob.fromFile(new File(imagePath1)),Blob.fromFile(new File(imagePath2)))
    print(res)
  }


}
