package com.example


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.io.{File, PrintWriter}

class CsvWriter {
  def writeToSource(csvToList:List[String],csvPath: String): Unit = {
    println("Starting writing csv to the following path: " + csvPath)

    val writer : PrintWriter = new PrintWriter(new File(csvPath))
    for(line <- csvToList){
      writer.println(line)
    }
    writer.close()
    println("Writtng csv finished")
  }


  //calling .collect() on rdd will cause java.lang.OutOfMemoryError: Java heap space
  def writeRDD(rdd:RDD[((String,String),Int)],csvPath:String,spark:SparkSession): Unit = {

    println("Starting writing RDD in csv format to the following path: " + csvPath)

    println("The rdd has " + rdd.count() +" lines")


    val schema = StructType(Seq(
      StructField("col1", IntegerType, nullable = true),
      StructField("col2", IntegerType, nullable = true),
      StructField("col3", IntegerType, nullable = true)
    ))

    // Converte l'RDD in un RDD di righe
    val rowRDD = rdd.map { case ((str1, str2), intVal) => Row(str1.toInt, str2.toInt, intVal) }

    // Crea il DataFrame a partire dall'RDD di righe e dallo schema
    val dataframe = spark.createDataFrame(rowRDD.sortBy(_.getInt(0)), schema)
    dataframe.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "false")
      .save(csvPath)


    println("Writtng csv finished")
    println("Most buyed object: " + dataframe.tail(3))
  }

  def writeRDDWithPartitioner(partitionedRdd:RDD[(Int,Int,Int)],csvPath:String,spark:SparkSession): Unit ={

    println("Starting writing RDD in csv format to the following path: " + csvPath)
    println("The rdd has " + partitionedRdd.count() +" lines")

    val dataframe = spark.createDataFrame(
      partitionedRdd
        .sortBy(el=> el._1)

    )
    dataframe.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "false")
      .save(csvPath)


    println("Writtng csv finished")
    println("Most buyed object: " + dataframe.tail(3).toString)
  }

}
