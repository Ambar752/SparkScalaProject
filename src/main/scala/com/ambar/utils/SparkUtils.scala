package com.ambar.utils

import com.ambar.models.FileFormat.FileFormat
import com.ambar.models.Paths.Paths
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.ambar.models._


object SparkUtils {

    def getSparkSession(appName: String, master: String = "local[*]"): SparkSession = {
        SparkSession.builder()
          .appName(appName)
          .master(master)
          .getOrCreate()
    }

  //Example of Currying and Implicit Parameter
  def getFileData(basePath: Paths,format: FileFormat,fileName: String,options: Map[String,String])(implicit spark: SparkSession): DataFrame = {

    spark.read.format(FileFormat.getFormat(format))
      .options(options)
      .load(Paths.getPathString(basePath) + "\\" + fileName)

  }
}
