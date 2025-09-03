package com.example

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class PurchaseFourthVersion {



  def coPurchaseItems(items: RDD[String],size:Int, debugMode: Boolean,spark:SparkSession): RDD[(Int, Int, Int)] ={
    val filteredItems: RDD[String] = size match {
      case 1 => items.zipWithIndex().filter(_._2 % 4 == 0).map(_._1)
      case 2 => items.zipWithIndex().filter(_._2 % 2 == 0).map(_._1)
      case 3 => items.zipWithIndex().filter(_._2 % 4 != 3).map(_._1)
      case _ => items
    }
    //Given an RDD[String] We'll parse all as (O, P) Where, O is the order and P is the product
    val orderProductPair = filteredItems.map(e => {
      val tmp = e.toString().replace("[", "").replace("]", "").split(",")
      (tmp.head.toInt, tmp.tail.head.toInt)
    })

    val partitioned = orderProductPair.partitionBy(new OrderPartitioner(96))
    val partitionedRdd = partitioned.groupBy(e => e._1).map(e => e._2).map(e => {
      e.map(pair => {
        pair._2
      })
    }).flatMap(productIds => {
      for {
        x <- productIds
        y <- productIds
        if x < y
      } yield (x, y)
    }).groupBy(e => e).map(e => {
      val (x, y) = e._1
      (x, y, e._2.size)
    })


    if (debugMode) {
      //printing the results
      val n = 100
      println("Printing first " + n + " elements")
      partitionedRdd.take(n).foreach(el => {
        println("pair " + el._1.toString() + " occurrences: " + el._2)
        println("------------------------------")
      })
    }


    println("Finished copurchase analysis" + partitionedRdd.count() +" lines in total" )

     partitionedRdd
  }







}
