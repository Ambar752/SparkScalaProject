package com.ambar.utils

import org.apache.spark.sql.SparkSession

class customSparkSession(val appName: String, val master: String = "local[*]") {
    implicit def getSparkSession: SparkSession = {
      SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .getOrCreate()
    }
}
