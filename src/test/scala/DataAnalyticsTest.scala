import com.ambar.models.{FileFormat, Paths}
import com.ambar.utils.{DataAnalytics, DataLoader, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.ambar.utils.customSparkSession

class DataAnalyticsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  var sparkTest: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkTest = new customSparkSession("TestDataAnalytics").getSparkSession
    //sparkTest = SparkUtils.getSparkSession("TestDataAnalytics","local[*]")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sparkTest.stop()
  }

  before {
    val options = Map("inferSchema" -> "true","header" -> "true")
    DataAnalytics.txnDF = SparkUtils.getFileData(sparkTest,Paths.ResoureBasePath,FileFormat.CSV,"transactionsTest.csv",options)

  }

  test("Check the Bankwise Credits and Debits") {
    val inputToBeTested = DataAnalytics.getBankNameWiseCreditsNDebits(sparkTest)
    val ActualOutput: Seq[(String,Int,Int)] = inputToBeTested.collect().toSeq.map(r => (r.getString(0),r.getLong(1).toInt,r.getLong(2).toInt))
    val ExpectedOutput: Seq[(String,Int,Int)] = Seq(("Axis",444,477),("HDFC",723,411))
    ActualOutput shouldEqual ExpectedOutput
  }

}
