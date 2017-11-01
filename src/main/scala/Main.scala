import java.io.{File, FileWriter, PrintWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Sorting

/**
  * single machine SON without using SPARK
  * will also utilize Apriori
  * @param s support
  * @param p
  * @param baskets
  */
class SON(val s: Int, val p: Float, var baskets: List[List[Int]]) {

  def run(chunks: List[List[List[Int]]], k: Int, data: Set[List[Int]]): Set[List[Int]]={
    //phase 1
    var set = scala.collection.mutable.Set[List[Int]]()
    // similar to reduce
    for (c <- chunks) {
      val runner = new Apriori(c)
      var candidates: Set[List[Int]] = Set()
      val t = (p * s).toInt
      if (k == 1) {
        candidates = runner.filter(List(), k, t) // generate singleton
      }
      else candidates = runner.filter(runner.construct(data, k), k, t)
      if (candidates.nonEmpty) {
        for (c <- candidates) {
          set += c
        }
      }
    }
    val immuSet = Set() ++ set
    return getTrueFrequentItemsets(immuSet)
  }

  def runAll(): Unit = {
    val n = (1/p).toInt
    var chunks = baskets.grouped(n).toList
//    println(chunks)
    var k = 1
    var freqItem = run(chunks, k, Set[List[Int]]())
    while (freqItem.nonEmpty) {
      k += 1
      freqItem = run(chunks, k, freqItem)
    }
  }

  def containsAll(a: List[Int], b: List[Int]): Boolean = {
    for (ele <- b) {
      if (!a.contains(ele)) {
        return false
      }
    }
    return true
  }

  def getTrueFrequentItemsets(candidates: Set[List[Int]]): Set[List[Int]] ={
    var result: Set[List[Int]] = Set()
    for (can <- candidates) {
      var sc = 0
      for (basket <- baskets) {
        if (containsAll(basket, can)) {
          sc += 1
        }
      }
      if (sc >= s) {
        result ++= List(can)
      }
    }
    return result
  }

}

/**
  * Single machine Apriori
  * @param baskets
  */
class Apriori(val baskets: List[List[Int]]){
  def containsAll(a: List[Int], b: List[Int]): Boolean = {
    for (ele <- b) {
      if (!a.contains(ele)) {
        return false
      }
    }
    return true
  }

  def biggerThan(cand: List[Int], t: Int): Boolean = {
    var count = 0
    for (basket <- baskets) {
        if (containsAll(basket, cand)) {
          count += 1
        }
        if (count >= t) {
          return true
        }
    }
    return false
  }

  /**
    * count the pairs
    * @param candidate candidate k-tuples
    * @param k
    * @param t threshold
    * @return truly frequent k-tuples that above the threshold
    */
  def filter(candidate: List[List[Int]], k: Int, t: Int): Set[List[Int]] = {
//    println(baskets)
    if (k == 1) {
      // count the items from all iems
      var count: Map[List[Int], Int] = Map()
      for ( basket  <- baskets) {
        for (i <- basket) {
          if (! count.contains(List(i))) count += (List(i) -> 1)
          else {
            count = count.updated(List(i), count(List(i)) + 1)
          }
        }
      }
      return count.filter(_._2 >= t).keySet
    }
    else {
      var filtered = scala.collection.mutable.Set[List[Int]]()
      for (cand <- candidate) {
        if (biggerThan(cand, t)) {
          filtered += cand
        }
      }
      return Set() ++ filtered
    }
  }

  def isValid(candidate: List[Int], frequent: Set[List[Int]], k: Int) : Boolean = {
    for (c <- candidate.combinations(k-1)) {
      var isContain = false
      for (f <- frequent) {
        if (containsAll(f, c))
          isContain = true
      }
      if (!isContain) return false
    }
    return true
  }

  /**
    * construct k-tuples candidate set
    * @param frequent frequent items of k-1
    * @param k
    * @return candidate pairs of k
    */
  def construct(frequent: Set[List[Int]], k: Int): List[List[Int]] = {
    var set = scala.collection.mutable.Set[Int]()
    for (tupled <- frequent) {
      set ++= tupled
    }
    var candidates = set.toList.combinations(k).toList.filter(isValid(_, frequent, k))
    return candidates
  }

  def generateAllCandidatePairs(t: Int): List[List[Int]] = {
    var k = 1
    var l = filter(List(), k, t) // generate singleton
    var result: List[List[Int]] = List()
    result ++= l
    k = k + 1
    var c = construct(l, k)
    while (c.nonEmpty) {
      k += 1
      l = filter(c, k, t)
      result ++= l
      if (l.nonEmpty) c = construct(l, k)
    }
    return result
  }

}

object Main {
  def containsAll(a: List[Int], b: List[Int]): Boolean = {
      for (ele <- b) {
        if (!a.contains(ele)) {
          return false
        }
      }
      return true
  }

  def printList(args: TraversableOnce[_]): Unit = {
    for (arg <- args.toList) {
      print(arg + " ")
    }
  }
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def appendToFile(fileName:String, textData:String) =
    using (new FileWriter(fileName, true)){
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
    }

  /** Phase 2 of distributed SON
    * Find true frequent itemset
    * @param chunk the subset of data
    * @param broadCastCandidates candidate items from phase 1
    * @return (c, value) pair c is candidate frequent itemset, value is support in this chunk
    */
  def countCandidate(chunk: List[List[Int]], broadCastCandidates: Array[List[Int]]): Iterator[(List[Int], Int)] = {
      import scala.collection.mutable.ListBuffer
      var l = new ListBuffer[(List[Int], Int)]()
      var count: Int = 0
      for (c <- broadCastCandidates) {
        for (itemSet <- chunk) {
          if (containsAll(itemSet, c)) {
            count += 1
          }
        }
        l.append((c, count))
        count = 0
      }
      return l.toIterator
  }

  /**
    * Phase 1 of distributed SON
    * Find itemsets frequent in this chunk
    * @param chunk the subset of data
    * @param broadCastData itemsets
    * @param k
    * @param p
    * @param support
    * @return candidate itemsets
    */
  def generateCandidate(chunk: List[List[Int]], broadCastData: Set[List[Int]], k: Int, p: Double, support: Int): Iterator[List[Int]] = {
    var _t = (p * support).toInt
    val runner = new Apriori(chunk)
    if (k == 1) {
      return runner.filter(List(), k, _t).toIterator
    } // generate singleton
    else {
      val constructed = runner.construct(broadCastData, k)
      return runner.filter(constructed, k, _t).toIterator
    }
  }


  /**
    * Generate all singleton, pair, triple pairs  . . . until there is no true k -tuples
    * output to File
    * @param basket our data
    * @param support
    * @param fileName output file name
    */
  def runAll(basket: RDD[List[Int]], support: Int, fileName: String): Unit = {
    val p = 0.2
    var k = 1
    var broadCastData: Set[List[Int]] = Set()
    var broadCastCandidate: List[Int] = List()
    var candidates =  basket.mapPartitions(iter => {
      generateCandidate(iter.toList, broadCastData, k, p, support)
    }).map((_, 1)).reduceByKey((_, _) => 1).keys

    val broadCastCandidates = candidates.collect()
    var frequent = basket.mapPartitions(iter => {
      countCandidate(iter.toList, broadCastCandidates)
    }).reduceByKey(_ + _).filter(_._2 >= support)

    var freqentSet = frequent.keys.collect()
    freqentSet = Sorting.stableSort(frequent.keys.collect(), (x: List[Int], y: List[Int]) => x(0) < y(0))
    broadCastData = freqentSet.toSet

    while (broadCastData.nonEmpty) {
      var str = StringBuilder.newBuilder
//      if (k != 1) str.append("\n")
      freqentSet.foreach(l => str.append(l.mkString("(", ", ", ")")).append(", "))
      str.deleteCharAt(str.length() - 1)
      str.deleteCharAt(str.length() - 1)
      appendToFile(fileName, str.toString())
      k += 1
      candidates = basket.mapPartitions(iter => {
        generateCandidate(iter.toList, broadCastData, k, p, support)
      }).map((_, 1)).reduceByKey((_, _) => 1).keys

      val broadCastCandidates = candidates.collect()

      frequent = basket.mapPartitions(iter => {
        countCandidate(iter.toList, broadCastCandidates)
      }).reduceByKey(_ + _).filter(_._2 >= support)

      freqentSet = frequent.keys.collect().map(_.sorted)
      for (i <- k to 1 by -1) {
        freqentSet = Sorting.stableSort(freqentSet, (x: List[Int], y: List[Int]) => x(i-1) < y(i-1))
      }
      broadCastData = freqentSet.toSet
    }
  }


  def main(args: Array[String]) {
    // Load the text into a Spark RDD, which is a distributed representation of each line of text

    val caseNum = args(0).toInt
//    val user = "ml-1m/users.dat"
    val user = args(2)
//    println("user path" + user)

    // UserID::Gender::Age::Occupation::Zip-code
    // 1::F::1::10::48067

//    val ratings = "ml-1m/ratings.dat"
    val ratings = args(1)
//    println("rating path" + ratings)

    // UserID::MovieID::Rating::Timestamp

    val support = args(3).toInt

    val numPartition = 5

    var fileName = ""
    if (caseNum == 1)  fileName = "YuHsiang_Tsai_SON.case1_" + support + ".txt"
    else  fileName = "YuHsiang_Tsai_SON.case2_" + support + ".txt"

    val conf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)

    val userRDD = sc.textFile(user, numPartition)
    val ratingRDD = sc.textFile(ratings, numPartition)
    val userGenderRDD = userRDD.map(line => line.split("::").slice(0, 2)).map(user => (user(0).toInt, user(1)))
    // (Int, String) (userID, Gender)

    val userMovieRDD = ratingRDD.map(line => line.split("::").slice(0, 2)).map(number => (number(0).toInt, number(1).toInt))
    // (Int, Array(Int)) (userID, movieID)
//    printList(userGenderRDD.take(10))
//    printList(userMovieRDD.take(10))
    val maleId = userGenderRDD.filter( t => t._2.equals("M"))
//    println(maleId.take(3))
    val femaleId = userGenderRDD.filter(t => t._2.equals("F"))

    val  basket = maleId.join(userMovieRDD).map(t => (t._1, t._2._2)).groupByKey().map(t => t._2.toList)
    val  basket2 = femaleId.join(userMovieRDD).map(t => (t._2._2, t._1)).groupByKey().map(t => t._2.toList)
    if (caseNum == 1) {
      runAll(basket, support, fileName)
    }
    else {
      runAll(basket2, support, fileName)
    }

  }
}




    // create test samples
//    val data = List(List(1,2,3), List(1,2,3,6), List(1,4,3), List(2, 3,5), List(1,4,5), List(2,5), List(1,2,3,5), List(3,2))
//    val runner = new Apriori(data)

//    val l = runner.filter(List(), 1, 3)
//    println(runner.construct(l, 2))
//    val c = runner.construct(l, 2)
//    val l2 = runner.filter(c, 2, 3)
//    println(runner.construct(l2, 3))
//    var t = 3

//    println(runner.generateAllCandidatePairs(t))

//    val distributed = new SON(3, 0.2f, data)
//    distributed.runAll()

