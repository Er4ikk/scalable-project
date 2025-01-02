package com.example

import java.io.PrintWriter

class CsvWriter {
  def writeToSource(csvToList:List[String],csvPath: String): Unit = {
    println("Starting writing csv to the following path: " + csvPath)
    val writer : PrintWriter = new PrintWriter(csvPath)
    for(line <- csvToList){
      writer.println(line)
    }
    writer.close()
    println("Writtng csv finished")
  }

}
