package com.example

import scala.io.Source

class CsvReader {
  def readFromSource(csvPath:String):List[String] = {
    println("Starting reading csv from the following path: " + csvPath)
  val file = Source.fromFile(csvPath)
    val csvIterator : Iterator[String] = file.getLines()

    val csvToList: List[String] = csvIterator
      .toList
    println("Got " + csvToList.length +" lines" )
/*
  for(line <- file.getLines()){
   csvToList= csvToList.appended(line)
  }

 */
    println("Finished reading")
    return  csvToList
  }

}

