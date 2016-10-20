// spamFilter.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object spamFilter {

  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {
	    //Read all the text files within a directory named filesDir
	    val allTxtFiles = sc.wholeTextFiles(filesDir)
	    //Number of files in nbFiles
	    val nbFiles = alltxtFiles.count()
	    //Split text files in set of unique word
	    //( File1 => [word1,word2, ... ], File2 => ... ) with distinct words
	    var files = allTxtFiles.map(e => (e._1, e._2.split("\\s+").distinct()))

	    //Create array with non informative word (scala world)
	    val nonInformativeWordsScala = Array(".",":",","," ","/","\\","-","","(",")","@")
	    //Transform nonInformativeWords in RDD (big data world)
	    val nonInformativeWords = sc.parallelize(nonInformativeWordsScala)

	    //Remove non informative words
	    files = files.map(e => (e._1, e._2.flatMap(w => if !nonInformativeWords.contains(w) w)))

	    //Create a list of (word1, 1) for each file
      //And apply distinct to have only one word per file
	    val wordsInEachFile = files.map(e => e._2.map(w => (w ,1 )).distinct())
      //reduce by key to have the occurency because if the word is in the
      //file there is one couple (w, 1)
      //(word, nb occurence)
      val wordDirOccurency = wordsInEachFile.reduceByKey(_+_)
    // Code to complete...

    //(probaWord, nbFiles)
  }*/


  /*def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {

  // Code to complete...

  }*/

  def main(args: Array[String]) {

	val conf = new SparkConf().setAppName("Simple Application")
  	val sc = new SparkContext(conf)
	println("Hello World")
  }

} // end of spamFilter
