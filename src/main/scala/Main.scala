package com.example
object Main {
  def main(args: Array[String]): Unit = {

    val csvPath:String ="/home/giulio/IdeaProjects/scalable-project/data/example.csv"
    val csvOut:String ="/home/giulio/IdeaProjects/scalable-project/data/out.csv"

    //this is for reading a csv
    val csvreader : CsvReader = new CsvReader()
    val csvToList: List[String] = csvreader.readFromSource(csvPath)
    println(csvToList)

    //first version of co-purchase
    val copurchase : PurchaseFirstVersion = new PurchaseFirstVersion()
    val result = copurchase.coPurchaseItems(csvToList)
    val list = normalizeOutput(result)




    //this is for writing a csv
    val csvWriter:CsvWriter = new CsvWriter()
    csvWriter.writeToSource(list, csvOut)

  }

  def normalizeOutput(result:Map[(String,String),Int]):List[String]={
    return result
      .map(el => (el._1.productIterator.mkString(","),el._2.toString).productIterator.mkString(","))
      .toList

  }


}