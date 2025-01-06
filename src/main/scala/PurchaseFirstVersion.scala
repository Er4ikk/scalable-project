package com.example

class PurchaseFirstVersion {

    def coPurchaseItems(items:List[String]): Map[(String,String),Int] ={

        //grouping items by their order number
         val groupedItemsByOrder :  Map[String,List[String]] = items
          .map(el => el.split(","))
          .groupBy(seq => seq(0))
          .map(el => (el._1,el._2.flatMap(el => el).filter(item => item != el._1)))

        //printing the results
        groupedItemsByOrder.foreach(el => {
            println("order number: " + el._1)
            println("items: " + el._2.mkString("-"))
            println("------------------------------")
        })

        //pairing objects
        val pairedObjects:Map[String,List[(String,String)]] = groupedItemsByOrder
          .map(el => (el._1,createCouples(el._2)))

        //counting occurences
        val pairedObjectsGroupedByOccurences:Map[(String,String),Int]=
            pairedObjects
              .map(el => el._2)
              .flatten
              .map(el => (el,1))
              .groupBy(el => el._1)
              .map(el => (el._1,el._2.size))

        return pairedObjectsGroupedByOccurences


    }

    def createCouples(list:List[String]):List[(String,String)]={
        val uniquePairs: List[(String, String)] = for {
            (x, idxX) <- list.zipWithIndex
            (y, idxY) <- list.zipWithIndex
            if idxX < idxY
        } yield (x, y)

        return uniquePairs
    }



}
