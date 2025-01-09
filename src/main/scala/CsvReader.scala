package com.example

import scala.io.Source

class CsvReader {

  def readFromSourceVariableSize(csvPath: String,size:Int): List[String] = {
    println("Starting reading csv from the following path: " + csvPath)
    val file = Source.fromFile(csvPath)
    var inputSize=Source.fromFile(csvPath).getLines().size
    val csvIterator: Iterator[String] = file.getLines()



    size match{
      //a fourth of the input size
      case 1 => inputSize = inputSize/4

      //half of the input size
      case 2 => inputSize= inputSize/2

      //three quarters of the input size
      case 3 => inputSize = inputSize/2 + inputSize/4

      //all input
      case _ => inputSize = inputSize
    }

    val csvToList: List[String] = csvIterator.take(inputSize).toList
    println("Got " + csvToList.length + " lines")
    println("Finished reading")
     csvToList
  }


  def readFromSourceVariableSizeToVector(csvPath: String,size:Int): Vector[String] = {
    println("Starting reading csv from the following path: " + csvPath)
    val file = Source.fromFile(csvPath)
    var inputSize=Source.fromFile(csvPath).getLines.size
    val csvIterator: Iterator[String] = file.getLines()



    size match{
      //a fourth of the input size
      case 1 => inputSize = inputSize/4

      //half of the input size
      case 2 => inputSize= inputSize/2

      //three quarters of the input size
      case 3 => inputSize = inputSize/2 + inputSize/4

      //all input
      case _ => inputSize = inputSize
    }

    val csvToList: Vector[String] = csvIterator.take(inputSize).toVector
    println("Got " + csvToList.length + " lines")
    println("Finished reading")
    csvToList
  }



}

