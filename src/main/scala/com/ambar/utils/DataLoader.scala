package com.ambar.utils

import com.ambar.models.{FileFormat, Gender, Paths}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.ambar.models.actors._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.expr

import java.sql.Date
import scala.util.Random

object DataLoader extends SparkSessionProvider {

     override implicit val appName: String =  "DataLoader"
     override implicit val master: String = "local[*]"

     def getRandomDate(seedDate: Date, maxDaysFromSeedDate: Int): Date = {
       val random = new Random()
       Date.valueOf(seedDate.toLocalDate.plusDays(random.nextInt(maxDaysFromSeedDate)))
     }

     def getBankData(implicit spark: SparkSession) :DataFrame = {

       val bankData = Seq(Bank(1,"HDFC","Mumbai","India"),
                          Bank(2,"Axis","Delhi","India"),
                          Bank(3,"ICICI","Bangalore","India")
                          )

       spark.createDataFrame(bankData)
     }

    def getBranchData(implicit spark: SparkSession) :DataFrame = {

      val branchData = Seq(Branch(10,1,"Padwal Nagar","H1001"),
                           Branch(11,1,"Shivaji Nagar","H1002"),
                           Branch(12,1,"Kisan Nagar","H1003"),
                           Branch(13,1,"Gokul Nagar","H1004"),
                           Branch(14,2,"Shree Nagar","A1001"),
                           Branch(15,2,"Abhudaya Nagar","A1002"),
                           Branch(16,2,"Prem Nagar","A1003"),
                           Branch(17,3,"Buddh Nagar","I1001"),
                           Branch(18,3,"Mahavir Nagar","I1002"),
                           )
      spark.createDataFrame(branchData)
    }

  def getCustomerData(implicit spark: SparkSession): DataFrame = {
    var customerData : Seq[Customer] = Seq.empty;
    customerData     = Seq(Customer(100,"Ambar",Date.valueOf("1988-12-20"),Gender.MALE.toString,"Mumbai","20001"),
                           Customer(101,"Swati",Date.valueOf("1992-12-20"),Gender.FEMALE.toString,"Mumbai","20002"),
                           Customer(102,"Kiran",Date.valueOf("1970-12-20"),Gender.FEMALE.toString,"Mumbai","20003"),
                           Customer(103,"Ram",Date.valueOf("1953-12-20"),Gender.MALE.toString,"Mumbai","20004"),
                           Customer(104,"Ajay",Date.valueOf("1989-12-20"),Gender.MALE.toString,"Mumbai","20005"),
                           Customer(105,"Cystal",Date.valueOf("1995-12-20"),Gender.FEMALE.toString,"Mumbai","20006"),
                           Customer(106,"Pratibha",Date.valueOf("1985-12-20"),Gender.FEMALE.toString,"Mumbai","20007"),
                           Customer(107,"Sanjay",Date.valueOf("1983-12-20"),Gender.MALE.toString,"Mumbai","20008"),
                           Customer(108,"Nandu",Date.valueOf("1983-12-21"),Gender.MALE.toString,"Mumbai","20009"),
                           Customer(109,"Binni",Date.valueOf("1983-12-21"),Gender.FEMALE.toString,"Mumbai","20010"),
                           Customer(110,"Sonali",Date.valueOf("1987-12-22"),Gender.FEMALE.toString,"Mumbai","20011"),
                           Customer(111,"Shivangi",Date.valueOf("1996-12-22"),Gender.FEMALE.toString,"Mumbai","20012"),
                           Customer(112,"Dinky",Date.valueOf("1988-12-22"),Gender.FEMALE.toString,"Mumbai","20013"),
                           Customer(113,"Shrikant",Date.valueOf("1965-12-22"),Gender.MALE.toString,"Mumbai","20014"),
                           Customer(114,"Shahrukh",Date.valueOf("1965-12-23"),Gender.MALE.toString,"Mumbai","20015"),
                           Customer(115,"Salman",Date.valueOf("1966-12-23"),Gender.MALE.toString,"Mumbai","20016"),
                           Customer(116,"Sunil",Date.valueOf("1999-12-22"),Gender.MALE.toString,"Mumbai","20017"),
                           Customer(117,"Sandeep",Date.valueOf("1991-12-22"),Gender.MALE.toString,"Mumbai","20018"),
                           Customer(118,"Vikas",Date.valueOf("1993-12-22"),Gender.MALE.toString,"Mumbai","20019"),
                           Customer(119,"Nilesh",Date.valueOf("1997-12-22"),Gender.MALE.toString,"Mumbai","20020"),
                           Customer(120,"Somesh",Date.valueOf("1998-12-22"),Gender.MALE.toString,"Mumbai","20021"),
                           Customer(121,"Guddu",Date.valueOf("1973-12-22"),Gender.MALE.toString,"Mumbai","20022")
                          )

    spark.createDataFrame(customerData)

  }

  def getAccountData(implicit spark: SparkSession): DataFrame = {
    var accountData: Seq[Account] = Seq()
    //val spark = SparkUtils.getSparkSession("DataLoader")
    val spark = DataLoader.getSparkSession
    val listOfCustomers = getCustomerData(spark).select("CustomerID").collect().map(_.getInt(0)).toList
    var startAccountID = 1000
    var startBranchID = 10
    var currBranchID = startBranchID

    for(currcustID <- listOfCustomers) {

      if(currcustID%2 == 0) {
        currBranchID = startBranchID + startAccountID%9
        accountData = accountData  ++ Seq(Account(startAccountID,currcustID,currBranchID,"Savings",getRandomDate(Date.valueOf("2023-03-01"),300)))

        startAccountID +=1
        currBranchID = startBranchID + startAccountID%9

        accountData = accountData  ++ Seq(Account(startAccountID,currcustID,currBranchID,"Savings",getRandomDate(Date.valueOf("2023-03-01"),300)))

      } else {

        currBranchID = startBranchID + startAccountID%9
        accountData = accountData  ++ Seq(Account(startAccountID,currcustID,currBranchID,"Current",getRandomDate(Date.valueOf("2023-03-01"),300)))
      }
      startAccountID +=1
    }

    spark.createDataFrame(accountData)

  }

  def getTransactions(implicit spark: SparkSession): DataFrame = {
    val options = Map("inferSchema" -> "true", "header" -> "true")
    var transactions = SparkUtils.getFileData(Paths.ResoureBasePath, FileFormat.CSV, "transactions.csv", options)

    import spark.implicits._

    //How to Use Map function on a DataFrame
    val transactionsWrapped = transactions.map(row => Transaction(
      row.getAs[Int]("TxnID"),
      row.getAs[Int]("FromAccountID"),
      row.getAs[Int]("ToAccountID"),
      row.getAs[Int]("TxnAmount"),
      row.getAs[String]("TxnType"),
      row.getAs[Date]("TxnDate")
    ))

    transactionsWrapped.toDF()
  }


  def getDataFrameWithOption(getNoneDataFrame : Int) : Option[DataFrame] = {

      var output : Option[DataFrame] = None

      if(getNoneDataFrame == 0) {
        output = Some(DataLoader.getSparkSession.createDataFrame(Seq(("Ambar",35),("Swati",32))).toDF("Name","Age"))
      } else {
        output = None
      }

      output
  }



}
