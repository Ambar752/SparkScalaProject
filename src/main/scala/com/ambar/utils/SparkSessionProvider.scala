package com.ambar.utils

import org.apache.spark.sql.SparkSession
//A Generic Trait that can be extended by Any Object and
//just by setting abstract member appName, the Object can access Spark Session Object in the format
// <ObjectName>.getSparkSession
trait SparkSessionProvider {
   implicit val appName: String
   implicit val master: String
    def getSparkSession: SparkSession = {
      SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .getOrCreate()
    }
}
