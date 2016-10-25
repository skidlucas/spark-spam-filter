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
	    val nbFiles = allTxtFiles.count().toLong
	    //Split text files in set of unique word
	    //( File1 => [word1,word2, ... ], File2 => ... ) with distinct words
	    var allTxtSplitFiles = allTxtFiles.map(e => (e._1, e._2.split("\\s+").distinct.toList))

	    //Create array with non informative word (scala world)
      //Add "!"
	    val nonInformativeWordsScala = Array(".",":",","," ","/","\\","-","","(",")","@","!")

	    //Remove non informative words
      //Take care of empty entry in e._2
	    var cleanFiles = allTxtSplitFiles.map(e => (e._1, e._2.map(w => if (!nonInformativeWordsScala.contains(w)) w else "").filter(_ != "")))

	    //Create a list of (word1, 1) for each file
      //And apply distinct to have only one word per file
	    val wordsInEachFile = cleanFiles.map(e => e._2.map(w => (w ,1)).distinct).flatMap(e => e)
      //reduce by key to have the occurency because if the word is in the
      //file there is one couple (w, 1)
      //(word, occurence nb)
      val wordDirOccurency = wordsInEachFile.reduceByKey(_+_)
      //Create the rdd with (w, probability)
      //probability of a word is occurence number / nb files
      val probaWord = wordDirOccurency.map(e => (e._1, (e._2.toDouble / nbFiles)))
      //DEBUG
      //probaWord.collect().foreach(println)
      //DEBUG
      (probaWord, nbFiles)
  }


  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {
    //We'll take the left join of probaW (proba of the word occurs in a file) with probaWC (proba word occurs in a file of a class)
    //Using left join because we have a sum in mutual information formula
    //Output is like (Word, (probaOccurs, Some(probaInClass)))
    val probaOccursAndProbaInClass = probaW.leftOuterJoin(probaWC)
    //For each entry if the map has a proba for the class let it or put the default one if value is none
    //Output is like (Word, (probaOccurs, probaInClass or probaDefault))
    val probaOccursAndCleanProbaClass = probaOccursAndProbaInClass.map(x => if (x._2._2 == None) (x._1, (x._2._1, probaDefault)) else (x._1, (x._2._1, x._2._2.get)))
    //Apply the formula
    val res = probaOccursAndCleanProbaClass.mapValues({
        case (x ,y) =>  y * math.log(y /(x * probaC))
    })
    //return value
    res
  }

  def main(args: Array[String]) {
	   val conf = new SparkConf().setAppName("SpamFilter")
  	 val sc = new SparkContext(conf)
     //TEST probaWordDir
     //var res = probaWordDir(sc)("/tmp/tp4/*.txt")
     //TEST
     //TEST computeMutualInformationFactor
     //The test doesn't work because the scala version of spark doesn't support Map()
     //val WC = Map("Bonjour" -> 0.5, "Salut" -> 0.0)
     //val W = Map("Bonjour" -> 0.75, "Salut" -> 0.5)
     //val C = 0.5
     //val Default = 1.0
     //var res = computeMutualInformationFactor(sc.parallelize(WC.toSeq), sc.parallelize(W.toSeq), C, Default)
     //TEST
     //REAL main
     //Compute word probabilities for the ham directory
     val (probaWordHam, nbFilesHam) = probaWordDir(sc)("/tmp/ling-spam/ham/*.txt")
     //Compute word probabilities for the spam directory
     val (probaWordSpam, nbFilesSpam) = probaWordDir(sc)("/tmp/ling-spam/spam/*.txt")

     //We need to have the proba of ham and spam
     val totalFiles = (nbFilesHam + nbFilesSpam).toDouble
     val probaSpam = nbFilesSpam / totalFiles
     val probaHam = nbFilesHam / totalFiles

     //Compute proba (occurs and ham) for word in ham = probaWordHam * probaHam
     val occursAndHam = probaWordHam.map(e => (e._1, e._2 * probaHam))
     //Compute proba (occurs and spam) for word in spam = probaWordSpam * probaSpam
     val occursAndSpam = probaWordSpam.map(e => (e._1, e._2 * probaSpam))
     //Compute proba (doesn't occur and ham) for word in ham = (1 - probaWordHam) * probaHam
     //(1 - probaWordHam) because a file is either spam or ham
     val noOccursAndHam = probaWordHam.map(e => (e._1, (1.0 - e._2) * probaHam))
     //Compute proba (doesn't occur and spam) for word in spam = (1 - probaWordSpam) * probaSpam
     //(1 - probaWordSpam) because a file is either spam or ham
     val noOccursAndSpam = probaWordSpam.map(e => (e._1, (1.0 - e._2) * probaSpam))

     //store default value
     val default = 0.2/totalFiles
     //To retrieve all the words we need to perform a fullouterjoin
     //because some words are only in spam files or in ham files
     val allWords = occursAndHam.fullOuterJoin(occursAndSpam)
     //We need to put the default value is a value is zero
     val allWordsDefault = allWords.mapValues { case (h, s) => (h.getOrElse(default), s.getOrElse(default))}
     //To compute mutual information we need to have the proba of a word if it occurs whatever the class (probaW)
     //a word is ham or spam so the result is (word, p(occursAndHam) + p(occursAndSpam))
     val occursWords = allWordsDefault.mapValues { case (h, s) => h + s }
     val noOccursWords = occursWords.mapValues { 1.0 - _ }
     //Now we can calculate mutual information for every case (ham or spam and occurs or not)
     val mutualInformationMap = List(
       //Occurs and Ham
       computeMutualInformationFactor(occursAndHam, occursWords, probaHam, default),
       //Occurs and Spam
       computeMutualInformationFactor(occursAndSpam, occursWords, probaSpam, default),
       //doesn't occur and Ham
       computeMutualInformationFactor(noOccursAndHam, noOccursWords, probaHam, default),
       //doesn't occur and Spam
       computeMutualInformationFactor(noOccursAndSpam, noOccursWords, probaSpam, default)
     )
     //mutualInformationMap.reduce{ (e1, e2) => e1.join(e2).map(e => (e._1, e._2._1 + e._2._2))}.take(10).foreach(println)
     //We have a list that we need to reduce to obtain mutual information of a word
     //Using reduceLeft looks like a loop
     //each step, make a join to merge the word and sum the mutual information part
     val mutualInformationReduce = mutualInformationMap.reduce {(e1, e2) => e1.join(e2).map(e => (e._1, e._2._1 + e._2._2))}
     //Take 10 top words
     //Use a minus to have descending sort
     val topWords = mutualInformationReduce.sortBy(- _._2).take(10)
     //Print topWords
     topWords.foreach(println)
     //save in the file
     sc.parallelize(topWords).saveAsTextFile("/tmp/topWords.txt")
	   println("Program ended")
  }

} // end of spamFilter
