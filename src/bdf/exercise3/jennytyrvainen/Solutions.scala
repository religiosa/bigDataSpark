package bdf.exercise3.jennytyrvainen

   /* Exercise 2 command: 
    * bdf/spark-1.2.1-bin-hadoop2.4/bin/spark-submit --master "spark://ukko192.hpc.cs.helsinki.fi" \
    *  --class bdf.exercise3.jennytyrvainen.Solutions thirdWeek.jar
    */

import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/* Definitions of all other objects, classes, global functions etc go here */

/* Finally we have the Solutions object */

object Solutions{
 /* Definitions of other functions of Solutions go here */
  
  var replacement = 'a'
  
 val parserFactory = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
 val parser = parserFactory.newSAXParser()
 val source = new org.xml.sax.InputSource("https://en.wikipedia.org/wiki/Solanaceae")
 val adapter = new scala.xml.parsing.NoBindingFactoryAdapter
 val all = adapter.loadXML(source, parser)
 
 implicit def boolToInt(b: Boolean) =  if (b) 1 else 0  
 
 def readHTML(): Seq[(String)] = {
    
    // take only the innermost tags, where the content is. this has been just tried out and hardcoded then
    val html = all.child
    val body = html(1)
    val content = body.child(5)
    val firstHeading = content.child(7)
    val bodyContent = content.child(9)
    val mwContentText = bodyContent.child(7)
    //(╯°□°）╯︵ ┻━┻

    val p = (mwContentText \\ "p")
    val pText = p.map(x => x.text)
    println(p.size)
    // drop 1, because at the start there is a list of scientific names, which are not part of the main content
    return pText.drop(1)
  }
  
  def readWordsHTML(): Array[(String)] = {
    val html = all.child
    return html.text.split(' ').map(word => word.filter(_.isLetter)).filter(!_.equals(""))
  }
  
  def distOfSmallWords(rdd: RDD[(String)]): RDD[(Float)] = {
    val small = List("and", "or", "the", "a", "an", "at", "on", "in")
    val cleanLines = rdd.map(line => line.toLowerCase.split(' ').map(word => word.filter(_.isLetter)).filter(!_.equals("")))
    val smallPerP = cleanLines.map(line => line.filter(small.contains(_)))
    val dists = cleanLines.zip(smallPerP).map(x => x._2.length / x._1.length.toFloat)
    
    return dists
  }
  
  def hammingAll(s1: String, s2: String): Int = {
    if (s1.length == s2.length) return hamming(s1, s2)
    else return hammingDiffLen(s1, s2)
  }
  
  def hamming(s1: String, s2: String): Int = {
    return s1.zip(s2).map(x => !(x._1 equals x._2):Int).reduce(_+_)
  }

  // there is no sense in this, because if the word is very short, the hamming distance will be very short as well.
  // so this will actually favor short words, because the average of hamming distances of all the substrings will still be small.
  def hammingDiffLen(s1: String, s2: String): Int = {
    val sMax = if(s1.length > s2.length) s1 else s2
    val sMin = if(s1 equals sMax) s2 else s1
    val diff = sMax.length-sMin.length
    
    var sum = 0
    for ( i <- Range(0, diff)) {
      sum = (sum + hamming(sMax.drop(i).take(sMin.length), sMin)) / 2
    }
    return sum
  }
  
  def closestPoint(p: String, centers: Array[(String)]): String = {
    val plaa = centers.map(x => (x, hammingAll(x, p).toFloat)).sortBy(_._2).head._1
    return plaa
  }
  
  def average(ps: Iterable[(String)]): String = {
    // how the f should I calculate an average for strings, so that the result would be meaningful?
    // so I'll make something up
    // calculate the average string by combining each element with each and calculating new char for every letter
    //val plaa = ps.toArray.permutations.map(_ zip ps).toArray.flatMap(x => x.map(y => ((y._1.toFloat + y._2.toFloat) / 2).toChar)).mkString
    val plaa = ps.reduce(_+_)
    var avg = ""
    for (i <- Range(0, plaa.length)) {
      if(i % ps.toArray.length == 0) avg = avg + plaa.charAt(i).toString
    }
    return avg
  }
  
  def distance(centers: Array[(String)], newCenters: Array[(String)]):Float = {
    return (centers zip newCenters).map(x => hammingAll(x._1, x._2)).reduce(_+_) / centers.length.toFloat
  }
  
  def kmeans(rdd: RDD[(String)], K: Int, epsilon: Float): RDD[(String, Iterable[(String)])]= {
    val seed = 1
    var d = 100.0
    val maxIters = 40
    var i = 0
    var centers = rdd.takeSample(false, K, seed)
    var pointsGroup : RDD[(String, Iterable[(String)])] = null
    while (d > epsilon && i < maxIters) {
      var closest = rdd.map(p => (closestPoint(p, centers), p))
      pointsGroup = closest.groupByKey
      var newCenters = pointsGroup.mapValues(ps => average(ps)).values
      d = distance(centers, newCenters.collect)
      centers = newCenters.collect
      i = i+1
    }
    return pointsGroup
  }
  
  def readWrite(sc: SparkContext, in: String, out: String) = {
   val words = sc.textFile(in).map(x => x.split(';'))
   
   sc.broadcast(replacement)
   var rdd = words.map(x => x.map(y => changeLetter(y))) 
   println(rdd.first()(1) + ", " + rdd.first()(2))
   
   replacement = 'e'
   sc.broadcast(replacement)
   rdd = words.map(x => x.map(y => changeLetter(y))) 
    println(rdd.first()(1) + ", " + rdd.first()(2))
   
   replacement = 'i'
   sc.broadcast(replacement)
   rdd = words.map(x => x.map(y => changeLetter(y))) 
   println(rdd.first()(1) + ", " + rdd.first()(2))
   
   words.saveAsObjectFile(out)
 }
 
 def testSaved(sc: SparkContext, file: String) = {
   val result : RDD[(Array[(String)])]= sc.objectFile(file)
   println(result.first.head.toString + " " + result.first()(2))
 }
 
 def changeLetter(word: String) : String = {
   val r = scala.util.Random.nextInt(word.length)
   val replace = word.charAt(r)
   return word.replace(replace, replacement)
 }
 
 def exercise2(sc: SparkContext) = {
   readWrite(sc, "testi_in.txt", "testi_out.txt")
   testSaved(sc, "testi_out.txt")
 }
 
 def exercise3(sc: SparkContext) = {
   val p = readHTML
   val rdd = sc.parallelize(p)
   println(rdd.first)
   val dists = distOfSmallWords(rdd)
   //dists.saveAsTextFile("exe3results")
 }
 
 def exercise4(sc: SparkContext) = {
   val words = readWordsHTML
   // this was for testing purposes
   //val words = Array("jotain", "sanoja", "testaukseen", "jotta", "olis", "dataa", "tarpeeksi", "kun", "klustereita", "on", "kaksi")
   println(words.size)
   val rdd = sc.parallelize(words)
   
   //test with different K and save
//   kmeans(rdd, 3, 3).saveAsTextFile("exe4resK3") //maxiters 20 till next comment
//   kmeans(rdd, 5, 3).saveAsTextFile("exe4resK5")
//   kmeans(rdd, 10, 3).saveAsTextFile("exe4resK10")
//   kmeans(rdd, 20, 3).saveAsTextFile("exe4resK20")
//   kmeans(rdd, 50, 3).saveAsTextFile("exe4resK50")
//   kmeans(rdd, 50, 3).saveAsTextFile("exe4resK5040m")  //maxiters 40 here onwards
//   kmeans(rdd, 300, 3).saveAsTextFile("exe4resK300")  
//   kmeans(rdd, 1000, 2).saveAsTextFile("exe4resK1000")
//   kmeans(rdd, 2000, 2).saveAsTextFile("exe4resK2000")
//   kmeans(rdd, 4000, 1).saveAsTextFile("exe4resK4000")
 }
 
 def main(args: Array[String]) {

   val conf = new SparkConf().setAppName(getClass.getName).setMaster("local[2]")

   val sc = new SparkContext(conf)

   /* Run exercise2 code */

  // exercise2(sc)

   /* Run exercise3 code */

   //exercise3(sc)

   /* Run exercise4 code */

   //exercise4(sc)

 }

} 
