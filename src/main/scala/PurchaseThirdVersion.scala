package com.example

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class PurchaseThirdVersion {
  //set number of threads
  val numberOfProcessors:Int =Runtime.getRuntime.availableProcessors()
  println("found " + numberOfProcessors +" available")

  def coPurchaseItems(items: RDD[String],size:Int, debugMode: Boolean,spark:SparkSession): RDD[((String, String), Int)] = {
    println("Starting copurchase analysis fount " + items.count()  +"lines")

    val filteredItems:RDD[String] = size match {
      case 1 => items.zipWithIndex().filter(_._2 % 4 == 0).map(_._1)
      case 2 => items.zipWithIndex().filter(_._2 % 2 == 0).map(_._1)
      case 3 => items.zipWithIndex().filter(_._2 % 4 != 3).map(_._1)
      case _ => items
    }

    println("after filtering there are " + filteredItems.count() +" lines to process")
  /*
    val groupedItemsByOrder : RDD[((String,String),Int)] = filteredItems
      .filter(line => line.contains(","))
      .map(line => {

        val parts = line.split(",")
        (parts(0), parts(1)) // Mappa ogni riga in (id ordine, id prodotto)
      })
      .filter(pair => pair._1 != null && pair._2 != null)
      .groupByKey() // Raggruppa per id ordine

      .flatMap(order => {
        val productList = order._2.toList // Ottieni la lista dei prodotti per un ordine
        productList.combinations(2).flatMap(pair => { // Genera tutte le coppie di prodotti
          val first = pair(0)
          val second = pair(1)
          if (first.compareTo(second) < 0) {
            Seq(((first, second), 1))
          } else {
            Seq(((second, first), 1))
          }
        })
      })
      .reduceByKey(_ + _) // Conta le occorrenze di ogni coppia
*/

    // 54s -> 8108622
    // 115s-> 16217244
    // java.lang.OutOfMemoryError: Java heap space    -> 24325866

    val groupedItemsByOrder : RDD[((String,String),Int)] =
      filteredItems
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
        .map {
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





    println("Finished copurchase analysis" + groupedItemsByOrder.count() +" lines in total" )

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
