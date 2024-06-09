import com.ambar.models.{FileFormat, Paths}
import com.ambar.utils.{DataAnalytics, DataLoader, SparkSessionProvider, SparkUtils, customSparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataAnalyticsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter with SparkSessionProvider {

  implicit var sparkTest:SparkSession = _
  override implicit val appName: String = "TestDataAnalytics"
  override implicit val master: String = "local[*]"


  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkTest = new DataAnalyticsTest().getSparkSession
    //sparkTest = new customSparkSession("TestDataAnalytics").getSparkSession
    //sparkTest = SparkUtils.getSparkSession("TestDataAnalytics","local[*]")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sparkTest.stop()
  }

  before {
    sparkTest = new DataAnalyticsTest().getSparkSession
    val options = Map("inferSchema" -> "true","header" -> "true")
    DataAnalytics.txnDF = SparkUtils.getFileData(Paths.ResoureBasePath,FileFormat.CSV,"transactionsTest.csv",options)

  }

  test("Check the Bankwise Credits and Debits") {
    val inputToBeTested = DataAnalytics.getBankNameWiseCreditsNDebits(sparkTest)
    val ActualOutput: Seq[(String,Int,Int)] = inputToBeTested.collect().toSeq.map(r => (r.getString(0),r.getLong(1).toInt,r.getLong(2).toInt))
    val ExpectedOutput: Seq[(String,Int,Int)] = Seq(("Axis",444,477),("HDFC",723,411))
    ActualOutput shouldEqual ExpectedOutput

  }

}
