package com.example

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class PurchaseThirdVersion {
  //set number of threads
  val numberOfProcessors:Int =Runtime.getRuntime.availableProcessors()
  println("found " + numberOfProcessors +" available")
  val sc :SparkConf = new SparkConf().setAppName("copurchase-app").setMaster("local")

  def coPurchaseItems(items: Vector[String], debugMode: Boolean): RDD[((String, String), Int)] = {
    println("Starting copurchase analysis")

    // 54s -> 8108622
    // 115s-> 16217244
    // java.lang.OutOfMemoryError: Java heap space    -> 24325866

    val groupedItemsByOrder : RDD[((String,String),Int)] = new spark.SparkContext(sc).parallelize(items)



      .map(el => el.split(","))
      .groupBy(seq => seq(0))
      //introducing lazy list
      .mapValues({
        groupedItems =>
          //take the values regrouped and filter them avoiding flatten
          val filteredItems = groupedItems.flatMap(_.tail).toVector

          filteredItems.indices.flatMap({
            i =>
              (i + 1 until filteredItems.length)

                .map(
                  {
                    j => (filteredItems(i), filteredItems(j))
                  })
          })
      })
      //counting occurences

      .values
      .flatMap(el => el.toSeq)

      .groupBy(identity)
      .map{
        case (key, values) => (key, values.size)
      }












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

/*
  def coPurchaseItemHybrid(items:Vector[String], debugMode:Boolean):Map[(String,String),Int]={
    //71s->8108622
    val groupedItemsByOrder : Map[(String,String),Int] = new spark.SparkContext(sc).parallelize(items)

      .collect()
      .map(el => el.split(","))
      .groupBy(el => el(0))




      //introducing lazy list
      .mapValues({
        groupedItems =>
          //take the values regrouped and filter them avoiding flatten
          val filteredItems = groupedItems.flatMap(_.tail).toVector

          filteredItems.indices.flatMap({
            i =>
              (i + 1 until filteredItems.length)

                .map(
                  {
                    j => (filteredItems(i), filteredItems(j))
                  })
          })
      })

      //counting occurences
      .values
      .flatten
      .groupMapReduce(identity)(_=>1)(_ + _)




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

 */


}
