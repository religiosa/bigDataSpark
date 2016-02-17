package bdf.exercise2.jennytyrvainen

import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
      

object Solutions{
  
  
  implicit def boolToInt(b: Boolean) =  if (b) 1 else 0  

  // to the power of 2
  implicit class powerInt(i: Int) {
    def `²` :  Int = i * i
  }
    
  
      // exercise 2
   
    def exercise2(sc: SparkContext) : RDD[(Int, ((String, Int, Int), Iterable[(String, Int, String)]))] = {
      val ratings = sc.textFile("ratings.dat") //userID::movieID::rating::timestamp
      val movies = sc.textFile("movies.dat") //movieID::title::genres
      val users = sc.textFile("users.dat")  //userID::gender::age::occupation::zip-code
    
                                                                      //movieID        //title    //genre
      val movielist = movies.map(line => line.split("::")).map(movie => (movie(0).toInt, (movie(1), movie(2))))
    
                                                                      //userID      //gender  //age  //occupation
      val userlist = users.map(line => line.split("::")).map(user => (user(0).toInt, (user(1), user(2).toInt, user(3).toInt)))
    
                                                                            //movieID         //userID        //rating
      val ratingsmovies = ratings.map(line => line.split("::")).map(rating => (rating(1).toInt, (rating(0).toInt, rating(2).toInt)))
    
                                                                            //userID          //movieID        //rating
      val ratingsusers = ratings.map(line => line.split("::")).map(rating => (rating(0).toInt, (rating(1).toInt, rating(2).toInt)))
    
    
      // join with movieIDs
      val intermediate = movielist.join(ratingsmovies)
    
      // remove movieIDs with "value", then create new RDD with userID as a key
    
                                                          //userID  //movie   //rating  //genre
      val moviesWithRatings = intermediate.values.map(x => (x._2._1, (x._1._1, x._2._2, x._1._2))).groupByKey
    
      // join with userIDs
      val all = userlist.join(moviesWithRatings)
    
      // RDD(UserID, gender, age, occupation, Set(movie, rating, genre))
      return all
    }
    
    // exercise 3
    
    def exercise3(sc: SparkContext, rdd: RDD[(Int, ((String, Int, Int), Iterable[(String, Int, String)]))]) : Int = {
      //filter out all under 18(age==1) and all unemployed (19), retired (13), K-12 students(10), homemaker(9), other(0)
      val notEmployed = Array(19, 13, 10, 9, 0)
      val employedAdults = rdd.filter(x => x._2._1._2 != 1).filter(x => !notEmployed.contains(x._2._1._3))
    
      //count the movies
      return employedAdults.map(x => x._2._2.size).reduce(_ + _)
    }
    
    // exercise 4
    def exercise4(sc: SparkContext, rdd: RDD[(Int, ((String, Int, Int), Iterable[(String, Int, String)]))]) : RDD[(Int, Array[(String, Float)])] = {

      return  rdd.map(x => (x._1, (x._2._1._3, x._2._2)))      // take userID, age, Set(movie, rating, genre)
      .values                                                  // drop userID
      .flatMap(x => x._2.map(y => ((x._1, y._1), y._2))).groupByKey  // (age, movie), rating i.e. drop genre
      .mapValues(x => (x.reduce(_ + _).toFloat / x.size))            // calculate avg ratings
      .map(x => (x._1._1, (x._1._2, x._2))).groupByKey               // reorganize to have age group as first, then group by it
      .map(x => (x._1, x._2.toArray.sortBy(_._2)(Ordering[Float].reverse) // make an array of Set(movie, rating), order it by rating
        .take(10))).sortByKey()                                        //for each age group take 10 then sort the resulting RDD by age group                                                                
    }
    
    // exercise 5
    
    // https://en.wikipedia.org/wiki/Correlation_and_dependence
    
    def exercise5(sc: SparkContext, rdd: RDD[(Boolean, Boolean)]) : Double = {
      return pearson(rdd)
    }
    
    def pearson(rdd: RDD[(Boolean, Boolean)]) : Double = {
            //The values are binary, i.e. 0 or 1, but here in boolean 
      
      val rddInt = rdd.map(x => (x._1:Int, x._2:Int))
      
      // first calculate mean for both values in the tuple
      val mean = rddInt.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      
      // calculate covariance, i.e. the top part of formula
      val covariance = rddInt.map(x => (x._1 - mean._1)*(x._2 - mean._2)).reduce(_+_)
      
      // calculate standard deviations, which are needed in the bottom part
      val stds = rddInt.map(x => (x._1 - mean._1 `²`, x._2 - mean._2 `²`)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      
      // combine
      return covariance / sqrt(stds._1 * stds._2)
    }
    
    // exercise 6
    
    def exercise6(sc: SparkContext, rdd: RDD[(Int, ((String, Int, Int), Iterable[(String, Int, String)]))]) : Array[(Array[(Double)])] = {
    
      def favouriteGenre(a: Array[(String)]) : String = {
        return a.groupBy(identity).map(genres => (genres._1, genres._2.size)).toArray.sortBy(_._2).reverse.head._1
      }
    
      val usersGenres = rdd.map(x => (x._1, x._2._2)).flatMap(x => x._2.map(y => (x._1, y._3))) //.map(x => (x._1, favouriteGenre(x._2)))
      
      val usersFavouriteGenres = usersGenres.map(x => (x._1, x._2.split('|'))).map(x => (x._1, favouriteGenre(x._2)))
      println("Genres: " + usersFavouriteGenres.first)
    
      val usersOccupations = rdd.map(x => (x._1, x._2._1._3))
    
      // choose one occupation and one genre, calculate binary matrix, i.e. where the user has said genre, put true, otherwise false
      val r = getCorrelation(12, "Fantasy", usersOccupations, usersFavouriteGenres)  //this works yay! intermediate tests, but left it here, because incomplete
      
      /*
       * occupations: 12 = programmer, 9 = homemaker, 10 = K-12 student, 2 = artist, 15= scientists
       * genres: Horror, Sci-Fi, Children's, Documentary, Film-Noir
       */
      val testOccupations =  Array(2, 9)      // Array(2, 9, 10, 12, 15)
      val testGenres =  Array("Documentary", "Horror")           //Array("Children's", "Documentary", "Film-Noir", "Horror", "Sci-Fi")
      
      val plaa = testOccupations.permutations.map(x => (x zip testGenres)).toArray
      
      val corrs = plaa.map(x => x.map(y => getCorrelation(y._1, y._2, usersOccupations, usersFavouriteGenres)))
     
      return corrs
    }
    
    def getCorrelation(o: Int, g: String, oAll: RDD[(Int, Int)], gAll: RDD[(Int, String)]) : Double = {
        val result = oAll.map(x => {
          var y = false
          if (x._2 == o) y = true
          else y = false
          (x._1, y)
        }).join(gAll.map(x => {
          var y = false
          if (x._2 == g) y = true
          else y = false
          (x._1, y)
        })).values
      
      return pearson(result)
    }
    

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    /* Run exercise2 code */

    val rdd = exercise2(sc)
    println(rdd.first)

    /* Run exercise3 code */

    val amount = exercise3(sc, rdd)
     println("Adults have seen in total " + amount + " movies.")

    /* Run exercise4 code */

    val topTen = exercise4(sc, rdd)
    println("Top Ten: " + topTen.first) // returns tuple (int, array), testing

    /* Run exercise5 code */

    val bools = Array((true,true), (true,false), (false, false) ,(true, true), (true, false), (true,true))
    val booRDD = sc.parallelize(bools)
    val r = exercise5(sc, booRDD) 
    println("Pearson correlation for test RDD is: " + r)

    /* Run exercise6 code */

    val test = exercise6(sc, rdd)
  }

}
