package com.ambar
import com.ambar.utils.{DataAnalytics, DataLoader, SparkSessionProvider, SparkUtils, customSparkSession}
import com.ambar.models._
import org.apache.spark.sql.SparkSession

import java.sql.Date
import scala.util.Random

object Main extends SparkSessionProvider {
  override implicit val appName: String =  "Main1"
  override implicit val master: String  = "local[*]"
  def main(args: Array[String]): Unit = {

    //implicit val spark = new customSparkSession("MainApp").getSparkSession
    implicit var spark : SparkSession =  Main.getSparkSession
    val options = Map("inferSchema" -> "true","header" -> "true")
    DataLoader.getAccountData.show()
    DataAnalytics.getBankNameWiseCreditsNDebits.show()

//    SparkUtils.getFileData(Paths.ResoureBasePath,FileFormat.CSV,"transactions.csv",options).show()

  }


}
