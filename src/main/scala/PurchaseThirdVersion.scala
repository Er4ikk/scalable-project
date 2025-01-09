package com.example

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

class PurchaseThirdVersion {
  //set number of threads
  val numberOfProcessors:Int =Runtime.getRuntime.availableProcessors()
  println("found " + numberOfProcessors +" available")
  val sc :SparkConf = new SparkConf().setAppName("copurchase-app").setMaster("local["+numberOfProcessors+"]")

  def coPurchaseItems(items: Vector[String], debugMode: Boolean): Map[(String, String), Int] = {
    println("Starting copurchase analysis")

    //59s -> 7000000
    //69s -> 8000000
    //69s ->8108622
    // ->16217244 java.lang.OutOfMemoryError: Java heap space

    val groupedItemsByOrder : Map[(String,String),Int] = new spark.SparkContext(sc).parallelize(items)

    .collect()

      .map(el => el.split(","))
      .groupBy(seq => seq(0))
      //introducing lazy list
      .mapValues( {
        groupedItems =>
          //take the values regrouped and filter them avoiding flatten
          val filteredItems = groupedItems.flatMap(_.tail)

          filteredItems.iterator.zipWithIndex.flatMap( {
            case(item1,i) => filteredItems
              .iterator
              .drop(i+1)
              .map(item2 =>(item1,item2))
          })
      } )
      //counting occurences
      .values
      .flatten

      // count occurences to improve readibility
      //https://blog.genuine.com/2019/11/scalas-groupmap-and-groupmapreduce/
      .groupMapReduce(identity)(_=>1)(_ + _)
    /*
    val groupedItemsByOrder : Map[(String,String),Int] = new spark.SparkContext(sc).parallelize(items)

      //.collect()
      .map(el => el.split(","))
      .groupBy(seq => seq(0))
      //introducing lazy list
      .mapValues( {
        groupedItems =>
          //take the values regrouped and filter them avoiding flatten
          val filteredItems = groupedItems.flatMap(_.tail)

          filteredItems.iterator.zipWithIndex.flatMap( {
            case(item1,i) => filteredItems
              .iterator
              .drop(i+1)
              .map(item2 =>(item1,item2))
          })
      } )
      //counting occurences
      .values
      .groupBy(identity)
      .map { case (key, values) =>        // Map each group
        (key, values.size)                // Count occurrences (values.size)
      }
      .flatMap {
        case (iterator, count) => iterator.map(pair => (pair, count))
      }
      .reduceByKey(_ + _)
      .collect()
      .toMap


     */

    // count occurences to improve readibility
      //https://blog.genuine.com/2019/11/scalas-groupmap-and-groupmapreduce/
     // .groupMapReduce(identity)(_=>1)(_ + _)










    if(debugMode){
      //printing the results
      val n = 100
      println("Printing first "+ n +" elements")
      groupedItemsByOrder.take(n).foreach(el => {
        println("pair " + el._1.toString() + " occurrences: " + el._2  )
        println("------------------------------")
      })
    }






    println("Finished copurchase analysis")

    groupedItemsByOrder

  }


}
