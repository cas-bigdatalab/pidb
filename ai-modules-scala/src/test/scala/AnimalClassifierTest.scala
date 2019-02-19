import java.io.File
import org.junit.{Assert, Test}

import cn.aipm.image.DogOrCatClassifier
import cn.pidb.blob.Blob


class AnimalClassifierTest {

  @Test
  def test1():Unit={
    var imagePath1 = "C:\\Users\\hai\\Desktop\\cat.1.jpg"
    val plateExtractor = new DogOrCatClassifier()
    val res = plateExtractor.extract(Blob.fromFile(new File(imagePath1)))
    print(res)
  }

}
