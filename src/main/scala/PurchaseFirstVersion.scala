package com.example

class PurchaseFirstVersion {

    def coPurchaseItems(items:List[String],debugMode:Boolean): Map[(String,String),Int] ={
      println("Starting copurchase analysis")

      //grouping items by their order number <- if the input is over 5000000 java.lang.OutOfMemoryError: Java heap space 53s
      val groupedItemsByOrder : Map[(String,String),Int] = items
        .take(5000000)
        .map(el => el.split(","))
        .groupBy(seq => seq(0))
        //introducing lazy list
        .view
        .mapValues( {
          groupedItems =>
            //take the values regrouped and filter them avoiding flatten
            val filteredItems = groupedItems.flatMap(_.tail)

            filteredItems.iterator.zipWithIndex.flatMap( {
              case(item1,i) => filteredItems
                    .iterator
                    .drop(i+1)
                    .map(item2 =>(item1,item2))
            })
        } )

        //counting occurences
        .values
        .flatten
        .toSeq
        // count occurences to improve readibility
        .groupMapReduce(identity)(_=>1)(_ + _)

        //old version  <- if the input is over 3000000: java.lang.OutOfMemoryError 123s
      /*
        //grouping items by their order number
         val groupedItemsByOrder : Map[(String,String),Int] = items
           //.take(3000000)
           .map(el => el.split(","))
          .groupBy(seq => seq(0))

          .map(el => (el._1, el._2.flatten.filter(item => item != el._1)))
           //pairing objects for version-> java.lang.OutOfMemoryError: Java heap space
           .map(el => (el._1,
             /*
             (for {
             i <- el._2.indices
             j <- i + 1 until el._2.length
           } yield (el._2(i), el._2(j))).toList)
           */
             //pairing obkjects functional -version <- java.lang.OutOfMemoryError: Java heap space
             el._2.indices.flatMap({ i =>
               (i + 1 until el._2.length).map( { j =>
                 (el._2(i), el._2(j))
               })})
           ))
           //counting occurences
           .values
           .flatten
           .map(el => (el,1))
           .groupBy(el => el._1)
           .map(el => (el._1,el._2.size))

       */




      if(debugMode){
            //printing the results
        val n = 100
        println("Printing first "+ n +" elements")
            groupedItemsByOrder.take(n).foreach(el => {
                println("pair " + el._1.toString() + " occurrences: " + el._2  )
                println("------------------------------")
            })
        }






      println("Finished copurchase analysis")

      return groupedItemsByOrder


    }




}
