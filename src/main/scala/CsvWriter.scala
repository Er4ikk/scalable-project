package com.example

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD

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
  def writeRDD(rdd:RDD[((String,String),Int)],csvPath:String): Unit = {
    println("Starting writing RDD in csv format to the following path: " + csvPath)
    /*
    val spark: SparkSession = SparkSession.builder.master("local["+ (numberOfProcessors/2 )+"]").getOrCreate
    val dfWithoutSchema = spark.createDataFrame(rdd.map(el => (el._1._1,el._1._2,el._2)))
    dfWithoutSchema.write.mode("overwrite").csv(csvPath)

     */
    FileUtils.deleteQuietly(new File("output/"))
    rdd
      .coalesce(1)
      .map(el => el._1._1 +"," + el._1._2 + "," + el._2 )
      .saveAsTextFile("output")

    //removing hidden files and _success
    println("removing hidden files and _success")
    FileUtils.deleteQuietly(new File("output/_SUCCESS"))
    FileUtils.deleteQuietly(new File("output/.part-00000.crc"))
    FileUtils.deleteQuietly(new File("output/._SUCCESS.crc"))

    //renaming file into csv
    new File("output/part-00000").renameTo(new File("output/part-00000.csv"))


    println("Writtng csv finished")
  }

}
