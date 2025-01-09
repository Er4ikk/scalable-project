package main.scala

import com.example.{CsvReader, CsvWriter, PurchaseFirstVersion}

object Main {
  def main(args: Array[String]): Unit = {

    val CSV_PATH_ARGS:Int=0
    val INPUT_SIZE_ARGS:Int=1
    val EXECUTION_MODE_ARGS: Int=2
    val DEBUG_MODE_ARGS:Int=3

    var inputSize:Int=4
    var debugMode: Boolean=false
    var executionMode:Int =1
    var csvPath: String = ""
    val csvOut: String = "out.csv"

    //argument management
    if(!args.equals(null) && args.length > 0){
      //first we check if the csv input path is present
      if(args.length > CSV_PATH_ARGS && !args(CSV_PATH_ARGS).equals(null)){
        csvPath= args(CSV_PATH_ARGS)
      }

      //then we check if the input size is defined otherwise we process all the input
      if (args.length > INPUT_SIZE_ARGS && !args(INPUT_SIZE_ARGS).equals(null)) {
        inputSize =args(INPUT_SIZE_ARGS).toInt
      }

      //debug mode only for debugging and showing more info
      if(args.length > DEBUG_MODE_ARGS && !args(DEBUG_MODE_ARGS).equals(null) && args(DEBUG_MODE_ARGS).equals("-d")){
        debugMode=true
      }

      if(args.length > EXECUTION_MODE_ARGS && !args(EXECUTION_MODE_ARGS).equals(null)){
        executionMode = args(EXECUTION_MODE_ARGS).toInt
      }
    }

    val t1 = System.nanoTime

    //this is for reading a csv
    val csvReader : CsvReader = new CsvReader()

    //this is for writing a csv
    val csvWriter: CsvWriter = new CsvWriter()

    //approach management
    executionMode match{

      //first version of co-purchase
      case 1 =>{
        println("Using standard approach!")
        val csvToList: List[String] = csvReader.readFromSourceVariableSize(csvPath,inputSize)
        if(debugMode)
          println("items collected: " + csvToList)
        val copurchase: PurchaseFirstVersion = new PurchaseFirstVersion()
        val result = copurchase.coPurchaseItems(csvToList, debugMode)
        val list = normalizeOutput(result)
        csvWriter.writeToSource(list, csvOut)
      }


      //second version of co-purchase
      case 2=>{
        println("Using parallel collections")
        val csvToList: Vector[String] = csvReader.readFromSourceVariableSizeToVector(csvPath,inputSize)
        if(debugMode)
          println("items collected: " + csvToList)
        val copurchaseV2: PurchaseSecondVersion = new PurchaseSecondVersion()
        val resultV2 = copurchaseV2.coPurchaseItems(csvToList, debugMode)
        val listV2 = normalizeOutput(resultV2)
        csvWriter.writeToSource(listV2, csvOut)
      }

      case _ =>{
        println("Uknown value for execution mode: " + executionMode + " using standard approach" )
        val csvToList: List[String] = csvReader.readFromSourceVariableSize(csvPath,inputSize)
        if(debugMode)
          println("items collected: " + csvToList)
        //first version of co-purchase
        val copurchase: PurchaseFirstVersion = new PurchaseFirstVersion()
        val result = copurchase.coPurchaseItems(csvToList, debugMode)
        val list = normalizeOutput(result)
        csvWriter.writeToSource(list, csvOut)
      }
    }










    //nanoTime discussion -> https://stackoverflow.com/questions/37730808/how-i-know-the-runtime-of-a-code-in-scala
    val duration = (System.nanoTime - t1) / 1e9d

    println("total time for computation:  " + duration + "s")

  }

  private def normalizeOutput(result:Map[(String,String),Int]):List[String]={
     result
      .map(el => (el._1.productIterator.mkString(","),el._2.toString).productIterator.mkString(","))
      .toList

  }


}