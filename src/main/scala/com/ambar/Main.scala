package com.ambar
import com.ambar.utils.{DataAnalytics, DataLoader, SparkUtils, customSparkSession}
import com.ambar.models._

import java.sql.Date
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark = new customSparkSession("MainApp").getSparkSession

    val options = Map("inferSchema" -> "true","header" -> "true")
    DataAnalytics.getBankNameWiseCreditsNDebits.show()

//    SparkUtils.getFileData(Paths.ResoureBasePath,FileFormat.CSV,"transactions.csv",options).show()

  }
}
