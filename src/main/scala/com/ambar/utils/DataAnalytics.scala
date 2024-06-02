package com.ambar.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataAnalytics {

  //Example of Implicit Parameter
  implicit val spark:SparkSession = new customSparkSession("Analytics").getSparkSession

  var txnDF = DataLoader.getTransactions
  var accountDF = DataLoader.getAccountData
  var bankDF = DataLoader.getBankData
  var branchDF = DataLoader.getBranchData

  def getBankNameWiseCreditsNDebits(implicit spark: SparkSession): DataFrame = {

    txnDF.createOrReplaceTempView("txnTab")
    accountDF.createOrReplaceTempView("accountTab")
    bankDF.createOrReplaceTempView("bankTab")
    branchDF.createOrReplaceTempView("branchTab")

    val resultDF =
    spark.sql("SELECT ba.BankName, " +
              "SUM(CASE WHEN t.TxnType = 'Credit' THEN t.TxnAmount END) Credits, " +
              "SUM(CASE WHEN t.TxnType = 'Debit' THEN t.TxnAmount END) Debits " +
              " FROM txnTab t INNER JOIN accountTab a " +
              " ON(t.FromAccountID = a.AccountID) " +
              " INNER JOIN branchTab br " +
              " ON(a.BranchID = br.BranchID) " +
              " INNER JOIN bankTab ba " +
              " ON (br.BankID = ba.BankID) " +
              " GROUP BY 1 ORDER BY 1")

    spark.catalog.dropTempView("txnTab")
    spark.catalog.dropTempView("accountTab")
    spark.catalog.dropTempView("bankTab")
    spark.catalog.dropTempView("branchTab")

    resultDF

  }

}
