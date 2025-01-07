package main.scala

import com.example.{CsvReader, CsvWriter, PurchaseFirstVersion}

object Main {
  def main(args: Array[String]): Unit = {

    val CSV_PATH_ARGS:Int=0
    val INPUT_SIZE_ARGS:Int=1
    val DEBUG_MODE_ARGS:Int=2

    var inputSize:Int=4
    var debugMode: Boolean=false
    var csvPath: String = "/home/giulio/IdeaProjects/scalable-project/data/example.csv"
    val csvOut: String = "out.csv"

    if(!args.equals(null) && args.length > 0){
      //first we check if the csv input path is present
      if(args.length > 0 && !args(CSV_PATH_ARGS).equals(null)){
        csvPath= args(CSV_PATH_ARGS)
      }

      //then we check if the input size is defined otherwise we process all the input
      if (args.length > 1 && !args(INPUT_SIZE_ARGS).equals(null)) {
        inputSize =args(INPUT_SIZE_ARGS).toInt
      }

      //debug mode only for debugging and showing more info
      if(args.length > 2 && !args(DEBUG_MODE_ARGS).equals(null) && args(DEBUG_MODE_ARGS).equals("-d")){
        debugMode=true
      }
    }





    //this is for reading a csv
    val csvReader : CsvReader = new CsvReader()
    val csvToList: List[String] = csvReader.readFromSourceVariableSize(csvPath,inputSize)

    if(debugMode)
      println("items collected: " + csvToList)

    //first version of co-purchase
    val copurchase : PurchaseFirstVersion = new PurchaseFirstVersion()
    val result = copurchase.coPurchaseItems(csvToList,debugMode)
    val list = normalizeOutput(result)




    //this is for writing a csv
    val csvWriter:CsvWriter = new CsvWriter()
    csvWriter.writeToSource(list, csvOut)

  }

  def normalizeOutput(result:Map[(String,String),Int]):List[String]={
     result
      .map(el => (el._1.productIterator.mkString(","),el._2.toString).productIterator.mkString(","))
      .toList

  }


}