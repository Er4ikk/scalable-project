package com.example
object Main {
  def main(args: Array[String]): Unit = {

    val csvPath:String ="/home/giulio/IdeaProjects/scalable-project/data/example.csv"
    val csvOut:String ="/home/giulio/IdeaProjects/scalable-project/data/out.csv"

    //this is for reading a csv
    val csvreader : CsvReader = new CsvReader()
    val csvToList: List[String] = csvreader.readFromSource(csvPath)
    println(csvToList)


    //this is for writing a csv
    val csvWriter:CsvWriter = new CsvWriter()
    csvWriter.writeToSource(csvToList, csvOut)

  }
}