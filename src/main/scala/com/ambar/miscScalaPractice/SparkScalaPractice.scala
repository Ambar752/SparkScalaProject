package com.ambar.miscScalaPractice

import com.ambar.utils.{DataLoader, SparkSessionProvider}
import org.apache.spark.sql.functions.{broadcast, col, explode, split}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object SparkScalaPractice extends SparkSessionProvider with App {

  override implicit val appName: String = "SparkScalaPractice"
  override implicit val master: String = "local[*]"

  implicit val spark : SparkSession = SparkScalaPractice.getSparkSession

  def broadcastJoin = {
    val branchDF: DataFrame = DataLoader.getBranchData
    //val bankDF: DataFrame = broadcast(DataLoader.getBankData)
    val bankDF: DataFrame = DataLoader.getBankData

    //Enable Broadcast Join and How to set Spark Properties
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",10485760)
    var finalJoined1: DataFrame = branchDF.join(bankDF,branchDF("BankID") === bankDF("BankID"))
    finalJoined1.explain()

    //Disable Broadcast Join and How to set Spark Properties
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    var finalJoined2: DataFrame = branchDF.join(bankDF,branchDF("BankID") === bankDF("BankID"))
    finalJoined2.explain()
  }

  def smallMiscConcepts = {

    val branchDF: DataFrame = DataLoader.getBranchData
    val bankDF: DataFrame = DataLoader.getBankData

    //Prints the Number of Partitions
    print(branchDF.coalesce(2).rdd.getNumPartitions)
    print(branchDF.repartition(10,col("BankID")).rdd.getNumPartitions)

    //Convert Datastructure to DataFrame
    val NameAgeTuple = Seq(("Ambar",35),("Swati",32),("Loma",70))
    val NameAgeTupleDF = spark.createDataFrame(NameAgeTuple).toDF("Name","Age")

    //Convert Datastructure to RDD
    val NameAgeTupleRDD = spark.sparkContext.parallelize(NameAgeTuple)
    //Use OrderBy in Descending Order
    NameAgeTupleDF.orderBy(col("Age").desc).show()

    //Use of FlatMap in Scala to Flatten the Array
    var sampleArr: Array[String] = new Array[String](3)
    sampleArr = Array("1,2,3", "4,5,6", "7,8,9")
    println(sampleArr.flatMap(ele => ele.split(",")).toList)

    //Set Executor Memory Config on Spark Session and Print Same
    val sampleSparkSession = SparkSession.builder().appName("sample").config("spark.executor.memory","4g").getOrCreate()
    println(sampleSparkSession.conf.get("spark.executor.memory"))
    sampleSparkSession.stop()

  }

  def simpleExplodeAndUDFExample = {
    val studentSubjectMarksScored = Seq( ("Ambar",List("Maths:90","Eng:50","Marathi:55"),35),
                                         ("Swati",List("Maths:70","Eng:80","Marathi:40"),32),
                                         ("Loma",List("Maths:50","Eng:55","Marathi:90"),50))

    val studentSubjectMarksScoredrowData = studentSubjectMarksScored.map(row => Row(row._1,row._2,row._3))

    val schema = StructType(Array(StructField("Name",StringType,nullable = false),
                                  StructField("SubjectScore",ArrayType(StringType,containsNull = false),nullable = false),
                                  StructField("Age",IntegerType,nullable = false)))

    val studentSubjectMarksScoredrowRDD = spark.sparkContext.parallelize(studentSubjectMarksScoredrowData)

    val studentSubjectMarksScoredDF: DataFrame = spark.createDataFrame(studentSubjectMarksScoredrowRDD,schema)

    studentSubjectMarksScoredDF.withColumn("SubjectScore",explode(col("SubjectScore"))).show()

  }

  //broadcastJoin
  //smallMiscConcepts
  simpleExplodeAndUDFExample

}
