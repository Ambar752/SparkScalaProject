package com.ambar.miscScalaPractice

import com.ambar.utils.{DataLoader, SparkSessionProvider}
import org.apache.spark.sql.functions.{avg, broadcast, col, explode, lit, round, split, sum, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.LongAccumulator
import com.typesafe.config.{Config, ConfigFactory}

import java.sql.Date
import java.util.regex.Pattern

//How to define Custom Classes
class invalidIndexNumber(message: String, cause: Throwable=null) extends Exception(message,cause)
class delimiterNotFound(message: String, cause: Throwable=null) extends Exception(message,cause)

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

    //How to check Plan of a Dataframe
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

    //Convert RDD to DataFrame
    import spark.implicits._
    NameAgeTupleRDD.toDF("Name","Age").show()

    //How to get Schema of a Dataframe
    println(NameAgeTupleDF.schema.fieldNames.toList)


    //Use OrderBy in Descending Order
    NameAgeTupleDF.orderBy(col("Age").desc).show()

    //Use of FlatMap in Scala to Flatten the Array
    var sampleArr: Array[String] = new Array[String](3)
    sampleArr = Array("1,2,3", "4,5,6", "7,8,9")
    println(sampleArr.flatMap(ele => ele.split(",")).toList)

    //Set Executor Memory Config on Spark Session and Print Same
    val sampleSparkSession = SparkSession.builder().appName("sample").config("spark.executor.memory","4g").getOrCreate()
    println(sampleSparkSession.conf.get("spark.executor.memory"))


    //Example of FoldLeft and FoldRight
    val listOfColumns = Seq("Ambar","is","good","Developer")
    var finalSentence = listOfColumns.foldLeft("")((acc, word) => acc + " " + word)
    var finalSentence1 = listOfColumns.foldRight("")((word, acc) =>  word + " " + acc)
    println(finalSentence)
    println(finalSentence1)

    //Example of ScanLeft and ScanRight
    var finalSentence2 = listOfColumns.scanLeft("")((acc, word) =>  acc + " " + word)
    println(finalSentence2)
    var finalSentence3 = listOfColumns.scanRight("")((word, acc) =>  word + " " + acc)
    println(finalSentence3)


    //Example of using Map with DataFrame -> (Also Refer DataLoad.getTransactions)
    //Example of Converting RDD to DataFrame
    import spark.implicits._
    NameAgeTupleDF.repartition(3).rdd.map(r => ("Dr. " + r.getAs[String]("Name"),r.getAs[Int]("Age")) ).toDF("Name","Age").show()

    //Example of Accumulator
    val sum1: LongAccumulator = spark.sparkContext.longAccumulator("count accumulator")
    NameAgeTupleDF.repartition(3).foreach(r => sum1.add(r.getInt(1)))
    println("Sum of Ages is : " + sum1.value.toString)

    //Example of BroadCast variable
    //Example of using Map with DataFrame
    //Example of Converting RDD to DataFrame
    val myMap : Map[String,String] = Map("Ambar" -> "Acharya","Swati"->"Gupta","Loma"->"Ach")
    val myBroadcastedMap = spark.sparkContext.broadcast(myMap)

    NameAgeTupleDF.repartition(3).rdd.map(row =>
    {(row.getAs[String]("Name"),
      row.getAs[Int]("Age"),
      row.getAs[String]("Name") + " " + myBroadcastedMap.value.getOrElse(row.getAs[String]("Name"),"Unknown"))})
      .toDF("Name","Age","FullName").show(truncate = false)

    //Example of Regular Expression
    val pattern = Pattern.compile("^(.)+@(.)+$")
    println(pattern.matcher("ambar@ach").matches())


    //Example to Stop SparkSession
    sampleSparkSession.stop()
  }



  def simpleExplodeAndUDFExample = {

    val studentSubjectMarksScored = Seq( ("Ambar",List("Maths:90","Eng:50","Marathi:55"),35),
                                         ("Swati",List("Maths:70","Eng:80","Marathi:40"),32),
                                         ("Loma",List("Maths:50","Eng:55","Marathi:90"),50))

    //How to convert Transform and Prep a Seq using Map
    val studentSubjectMarksScoredrowData = studentSubjectMarksScored.map(row => Row(row._1,row._2,row._3))

    //How to use Structs
    val schema = StructType(Array(StructField("Name",StringType,nullable = false),
                                  StructField("SubjectScore",ArrayType(StringType,containsNull = false),nullable = false),
                                  StructField("Age",IntegerType,nullable = false)))

    //How to convert a Seq to RDD
    val studentSubjectMarksScoredrowRDD = spark.sparkContext.parallelize(studentSubjectMarksScoredrowData)

    //How to create Dataframe from RDD
    var studentSubjectMarksScoredDF: DataFrame = spark.createDataFrame(studentSubjectMarksScoredrowRDD,schema)

    //How to use Explode Function
    studentSubjectMarksScoredDF = studentSubjectMarksScoredDF.withColumn("SubjectScore",explode(col("SubjectScore")))

    def splitString(input: String, valueByIndex: Int): String = {

      //How to handle Custom Exception
      try {

      if(!input.contains(":")) {
        //How to throw a custom Exception
        throw new delimiterNotFound("Delimiter ':' not found !")
      }

      if(valueByIndex == 0) {
        return input.split(":")(0)
      } else if (valueByIndex == 1) {
        return input.split(":")(1)
      } else {
        throw new invalidIndexNumber("Index must be either 0 or 1")
      }
      } catch {
        //How to catch a Custom Exception
        //How to print StackTrace
        case i : invalidIndexNumber => i.printStackTrace(); return "Invalid Index !";
        case d : delimiterNotFound => d.printStackTrace(); return " ':' delimiter not found !"
      }

    }

    //How to define UDF which uses Scala Method as Input
    val splitStringUDF = udf(splitString _)

    studentSubjectMarksScoredDF = studentSubjectMarksScoredDF
                                 .withColumn("Subject",splitStringUDF(col("SubjectScore"),lit(2)))
                                 .withColumn("Marks",splitStringUDF(col("SubjectScore"),lit(1)))

    //How to use groupBy, alias, cast, agg, sum, round functions with Dataframe
    val studentWiseTotalMarksDF = studentSubjectMarksScoredDF
                                  .groupBy(col("Name"))
                                  .agg(sum(col("Marks")).cast("int").cast("string").alias("Total Marks"),
                                       round(avg(col("Marks")),2).alias("Avg Marks"))



    studentWiseTotalMarksDF.show()

  }


  def readProjectConfigs() : Unit = {

    //Always use a Source Root Path (i.e. Relative Path from Folder marked as Source Root)
    val config: Config = ConfigFactory.load("config/application.conf")

    //2 ways to Access Configs
    println(config.getString("app.server.host"))
    println(config.getConfig("app").getConfig("server").getString("host"))
  }

  def howToCollectDataFromDataFrame() : Unit = {

    val mytestData = DataLoader.getBankData
    println(mytestData.collect().map(r => (r.getString(1) -> r)).toMap.get("ICICI").get(0))
    println(mytestData.collect().map(r => (r.getString(1) -> r)).toMap.get("ICICI").get(1))
    println(mytestData.collect().map(r => (r.getString(1) -> r)).toMap.get("ICICI").get(2))
    println(mytestData.collect().map(r => (r.getString(1) -> r)).toMap.get("ICICI").get(3))

  }

  def equalityOfDataFramesForUnitTests() : Unit = {
     val actualData = Seq(("Ambar",35,"Software"),
                        ("Swati",32,"Banker"),
                        ("Loma",52,"HouseHolder"))

    val expectedData = Seq(("Loma",52,"HouseHolder"),
                          ("Swati",32,"Banker"),
                          ("Ambar",35,"Software"))

    val actualDF = spark.createDataFrame(actualData).toDF("Name","Age","Profession")

    //How to Apply Schema of One DataFrame on Another Dataframe
    //How to Unfold a List into Vargs using _*
    val expectedDF = spark.createDataFrame(expectedData).toDF(actualDF.schema.fieldNames: _*)

    //Below will be FALSE because Order of Collections on Both Side is Incorrect
    println(actualDF.collect().sameElements(expectedDF.collect()))

    //Below will be TRUE because we convert the Collection into Key-Value base Map and the use Key-based Match
    println(actualDF.collect().map(i => (i.getString(0) -> i)).toMap.get("Loma").sameElements(expectedDF.collect().map(i => (i.getString(0) -> i)).toMap.get("Loma")))

  }

  def howToHandleOptionType() : Unit = {

    //Do a Pattern Matching on an Option type
    DataLoader.getDataFrameWithOption(getNoneDataFrame = 0) match {
      case Some(data) => data.show()
      case None       => println("No Data returned by DataLoader.getDataFrameWithOption")
    }

    DataLoader.getDataFrameWithOption(getNoneDataFrame = 1) match {
      case Some(data) => data.show()
      case None       => println("No Data returned by DataLoader.getDataFrameWithOption")
    }

  }

  //broadcastJoin
  //smallMiscConcepts
  //simpleExplodeAndUDFExample
  //readProjectConfigs
  //howToCollectDataFromDataFrame
  //equalityOfDataFramesForUnitTests
  //howToHandleOptionType

}
