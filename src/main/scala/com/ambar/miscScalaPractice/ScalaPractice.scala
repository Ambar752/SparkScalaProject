package com.ambar.miscScalaPractice

import java.util
import scala.collection.{SortedSet, mutable}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.logging.log4j.LogManager

object ScalaPractice extends App {
   def dataStructures(): Unit = {

     //Normal Set
     var normalSet :Set[Int] = Set()
     normalSet = Set(1,2,3,3,4,4,4)
     println(normalSet)

     //TreeSet
     var mySortedSet: SortedSet[Int] = SortedSet()
     mySortedSet = SortedSet(6,5,4,15,15,15,12,11,11,10)
     println(mySortedSet)

     //Linked
     var myLinkedHashSet: mutable.LinkedHashSet[Int] = mutable.LinkedHashSet()
     myLinkedHashSet = mutable.LinkedHashSet(6,5,4,15,15,15,12,11,11,10)
     println(myLinkedHashSet)

     //Size Of Set
     println(myLinkedHashSet.size)

     //For Loop with Set
     for(ele <- myLinkedHashSet) {
       println("Set Element " + ele.toString)
     }






     //Append Lists
     var myList1: List[String] = List("Ambar", "Acharya")
     var myList2: List[String] = List("Swati", "Gupta")
     println(myList1 ++ myList2)

     //List Operations
     var myList3: List[Int] = List(1,2,3,4,5,6,7)

     //Remove List by Value
     println(myList3.filterNot(_ == 4))

     //Remove List by Value
     println(myList3.patch(4,Nil,1))

     //Update nth element with x Value
     println(myList3.patch(2,Seq(99),1))

     //Order a List by Descending
     println(myList3.sortWith(_ > _))

     //Size of List
     println(myList3.size)

     //For Loop with Set
     for(ele <- myList3) {
       println("List Element " + ele.toString)
     }







     //String Operations
     var myString: String = "Ambar"

     //Reverse a String
     println(myString.reverse)

     //Size of String
     println(myString.length)

     //Repeat a String 3 times
     println("Repeated" * 3)

     //Get First and Last Ocurrance of a Word
     println("Ambar likes Coding but Ambar likes Coding only when done with Scala".indexOf("Coding"))
     println("Ambar likes Coding but Ambar likes Coding only when done with Scala".lastIndexOf("Coding"))

     //Get the character at a particular position in the String
     println("Ambar".charAt(2))

     //Get the character at a particular position in the String
     println("Ambar".toUpperCase)

     //Get Absoulte Difference
     println(Math.abs(-1))

     //Convert a String to Set
     println("AmbAr".toSet)

     //Convert a String to Comma Delimited
     println("AmbAr".map(_.toString).mkString(","))

     //Convert a String to Date
     val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     val mydate    = formatter.parse("2024-01-01 12:00:00")
     println(mydate)






     //Declare Empty Arrays
     var myArrayEmpty: Array[String] = Array.empty[String]

     //Declare Non-Empty Arrays
     var myArrayWithASize: Array[Int] = new Array[Int](10)
     myArrayWithASize = Array(100,101,102,103,104)

     //Fill Array with 1
     var myOneArray: Array[Int] = Array.fill(5)(1)
     myOneArray.map("myOneArray :" + _.toString).foreach(println)

     //Size of Array
     println(myArrayWithASize.length)

     //Print ArrayElements
     myArrayWithASize.foreach(println)

     //Update Array
     myArrayWithASize(4) = 105

     //Print ArrayElements
     myArrayWithASize.foreach(println)

     //Sort Array in Descending Order
     myArrayWithASize.sortWith(_ > _).foreach(println)

     //Sort Array in Descending Order
     myArrayWithASize.sortBy(-_).foreach(println)

     //Perform Cube on each Element of Array and then do Sum of All Cubes
     var mybaseArray: Array[Int] = new Array[Int](3)
     mybaseArray = Array(2,3,4)
     val mybaseArrayCubed = mybaseArray.map(e => e*e*e)
     val SumOfCubes: Int  = mybaseArrayCubed.reduce(_+_)

     mybaseArrayCubed.foreach(println)

     println(SumOfCubes)

     //Loop Through Array with Index
     for((ele,i) <- mybaseArray.zipWithIndex) {
       println("Element " + ele.toString + " has Index " + i.toString)
     }

     //Loop Reverse Through Array with Index
     for(i <- (0 until mybaseArray.length).reverse) {
       println("Element " + mybaseArray(i) + " has Index " + i.toString)
     }








     //Define a Map
     var myMap: Map[Int,Int] = Map(100 -> 5, 101->4, 102->3, 103->2, 104->1)

     //Print Map
     println(myMap)

     //Sort a Map By Value Descending ??
     val myMap1 = myMap.toSeq.sortWith(_._1 > _._1).toMap

     //Loop through a Map
     for((k,v) <-  myMap1) {
       println("Key : " + k.toString + " and Value: " + v.toString)
     }

     //Overide a Immutable Map
     myMap = myMap + (100->6)

     //Define a mutable Map
     var myMutableMap : mutable.Map[Int,Int] = mutable.Map(100 -> 5, 101->4, 102->3, 103->2, 104->1)

     println(myMutableMap)

     //Loop through a Map
     for((k,v) <-  myMutableMap) {
       println("Key : " + k.toString + " and Value: " + v.toString)
     }

     // Update a Key of a Mutable Map
     println(myMutableMap)
     myMutableMap(103) = 11
     println(myMutableMap)

     //Add Element to a Mutable Map
     myMutableMap.put(105,0)
     println(myMutableMap)

     //Handle Non-Existing Key by getOrElse
     println(myMutableMap.put(100,myMutableMap.getOrElse(100,0)+1))
     println(myMutableMap.put(100,myMutableMap.getOrElse(100,0)+1))
     println(myMutableMap.put(100,myMutableMap.getOrElse(100,0)+1))
     println(myMutableMap.put(106,myMutableMap.getOrElse(106,0)-1))
     println(myMutableMap)





    //Miscellaneous

     //If Else Expression in Scala
     val hoursWorkedInOffice = 10
     val output = if(hoursWorkedInOffice <= 10) "Not Busy Day" else "Busy Day"
     println(output)

   }

  def mapDataStructures() : Unit = {
    var myNormalMap : Map[Int,Int] = Map(10->1, 9->2, 8->3, 7->4, 6-> 5)

    //Gives Result in Un-ordered Way
    myNormalMap.foreach(println)

    println("\n")

    var myLinkedHashMap : scala.collection.mutable.LinkedHashMap[Int,Int] = scala.collection.mutable.LinkedHashMap(10->1,8->3, 7->4,9->2, 6-> 5)

    println("\n")
    println("//Map maintains Insertion Order")

    //Map maintains Insertion Order
    myLinkedHashMap.foreach(println)

    println("\n")
    println("//Sort By Keys Asc")

    //Sort By Keys Asc
    myLinkedHashMap.toList.sortBy(_._1).foreach(println)

    println("\n")
    println("//Sort By Keys Desc")

    //Sort By Keys Desc
    myLinkedHashMap.toList.sortBy(-_._1).foreach(println)



    println("\n")
    println("//Sort By Values Asc")

    //Sort By Values Asc
    myLinkedHashMap.toList.sortBy(_._2).foreach(println)

    println("\n")
    println("//Sort By Values Desc")

    //Sort By Values Desc
    myLinkedHashMap.toList.sortBy(-_._2).foreach(println)

    var myTreeMap : scala.collection.mutable.TreeMap[Int,Int] = scala.collection.mutable.TreeMap(10->1,8->3, 7->4,9->2, 6-> 5)

    println("\n")
    println("//Map will do Natural Sort by Key")

    //Map will do Natural Sort by Key
    myTreeMap.foreach(println)


    println("\n")
    println("//Sort TreeMap By Key")

    //Sort TreeMap By Key
    myTreeMap.foreach(println)



    var myImmutableMap : Map[Int,Int] = Map(1->10,2->11)
    println("\n")
    println("//Add Element to Immutable Map")

    //Add Element to Immutable Map
    myImmutableMap += (3->12)
    myImmutableMap.foreach(println)


    println("\n")
    println("//Update Element to Immutable Map")
    //Update Element to Immutable Map
    myImmutableMap += (3->13)
    myImmutableMap.foreach(println)




    var mymutableMap : scala.collection.mutable.Map[Int,Int] = scala.collection.mutable.Map(3->10,4->11)
    println("\n")
    println("//Add Element to Immutable Map")

    //Add Element to Immutable Map
    mymutableMap += (5->12)
    mymutableMap.foreach(println)


    println("\n")
    println("//Update Element to Immutable Map")
    //Update Element to Immutable Map
    mymutableMap += (5->13)
    mymutableMap.foreach(println)

  }

  def listDataStructures() : Unit = {
    var myImmutableList : List[Int] = List(1,2,3)

    println("//Add Element to Immutable List\n")

    //Add Element to Immutable List
    myImmutableList = myImmutableList :+  4
    myImmutableList.foreach(println)

    println("//Remove Element from Immutable List by Position\n")

    //Remove Element from Immutable List by Position
    myImmutableList.patch(2,Nil,1).foreach(println)

    println("//Remove Element from Immutable List by Value\n")

    //Remove Element from Immutable List by Value
    myImmutableList.filterNot(_ == 2).foreach(println)




    var myMutableList : scala.collection.mutable.ListBuffer[Int] = scala.collection.mutable.ListBuffer(1,2,3)

    println("//Add Element to Mutable List\n")

    //Add Element to Mutable List
    myMutableList += 4
    myMutableList.foreach(println)



    println("//Remove Element from Mutable List by Value\n")

    //Remove Element from Mutable List by Value
    myMutableList -= 3
    myMutableList.foreach(println)

    println("//Remove Element from Mutable List by Position\n")

    //Remove Element from Immutable List by Position
    myMutableList.remove(2)
    myMutableList.foreach(println)

  }

  def myCustomLogging(): Unit = {
    val logger = LogManager.getLogger("My Logger")
    logger.error("Hello INFO Logger Done by Ambar !!!!")
    logger.error("Hello ERROR Logger Done by Ambar !!!!")
  }

  //dataStructures()
  //mapDataStructures()
  //listDataStructures()
  //myCustomLogging
}
