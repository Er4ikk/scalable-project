package main.scala

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.ForkJoinTasks

class PurchaseSecondVersion {

  def coPurchaseItems(items: Vector[String], debugMode: Boolean): Map[(String, String), Int] = {
    println("Starting copurchase analysis")


    //set number of threads
    val numberOfProcessors:Int =Runtime.getRuntime.availableProcessors()
    println("found " + numberOfProcessors +" available")
    ForkJoinTasks.defaultForkJoinPool.setParallelism(numberOfProcessors-2)

    //First Version of parallelizable copurchase
    //63s 5000000
    //55s 6000000
    //105s 7000000
    //java.lang.OutOfMemoryError: Java heap space if the input is above 7000000

    /*
    val groupedItemsByOrder : Map[(String,String),Int] = items
      .take(5000000)
      .par
      .map(el => el.split(","))
      .groupBy(seq => seq(0))
      //introducing lazy list
      .mapValues( {
        groupedItems =>
          //take the values regrouped and filter them avoiding flatten
          val filteredItems = groupedItems.flatMap(_.tail).toVector

          filteredItems.indices.par.flatMap( {
            i => (i+1 until filteredItems.length)
              .par
              .map(
                {
                  j=> (filteredItems(i),filteredItems(j))
                })
          })
      } )
      //counting occurences
      .values
      .flatten
      .toVector
      // count occurences to improve readibility
      //https://blog.genuine.com/2019/11/scalas-groupmap-and-groupmapreduce/
      .groupMapReduce(identity)(_=>1)(_ + _)

     */

    //second version of copurchase analisys
    //47s -> 5000000
    //68s -> 7000000
    //java.lang.OutOfMemoryError: Java heap space if the input is above 7000000
    val groupedItemsByOrder : Map[(String,String),Int] = items
      .take(7000000)
      .par
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
      .seq
      // count occurences to improve readibility
      //https://blog.genuine.com/2019/11/scalas-groupmap-and-groupmapreduce/
      .groupMapReduce(identity)(_=>1)(_ + _)










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
