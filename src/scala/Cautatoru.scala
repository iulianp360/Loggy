import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.Query
import org.apache.lucene.document.Document
import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.io.File
import org.apache.lucene.util.Version
import org.apache.lucene.index.DirectoryReader
import scala.annotation.meta.field
import java.io.BufferedReader
import scala.annotation.meta.field
import org.apache.lucene.queryparser.classic.QueryParser
import scala.annotation.meta.field
import org.apache.lucene.search.spell.LuceneDictionary
import org.apache.lucene.search.suggest.InputIterator
import org.apache.lucene.search.TermQuery
import org.apache.lucene.index.Term
import org.apache.lucene.index.MultiFields
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.index.DocsEnum
import org.apache.lucene.util.BytesRef
import org.apache.lucene.util.NumericUtils
import org.apache.lucene.search.NumericRangeQuery
import scala.math.Ordering
import java.math.BigDecimal

object Cautatoru {

  val indexFolder = "c://temp//bkp//index//"
  val reader = DirectoryReader.open(FSDirectory.open(new File(indexFolder)))

  val searcher = new IndexSearcher(reader)
  val analyzer = new StandardAnalyzer(Version.LUCENE_4_10_2)
  val ipParser = new QueryParser(Version.LUCENE_4_10_2, "ip", analyzer)
  val statusParser = new QueryParser(Version.LUCENE_4_10_2, "status", analyzer)

  def main(args: Array[String]): Unit = {
//    display(maximumIp)
//
//    display(byIp("178.154.179.250"))
//    display(byIp("213.175.86.38"))
//    display(byIp("66.249.74.147"))
    
//    globalStatuses
    
    statusSumByIp("66.249.74.147")
  }
  
  def display(ip: IpSum) = println(s"Ip ${ip.ip} has ${ip.bytes.setScale(0)} bytes found in ${ip.time} ms. \n")
  
  def globalStatuses() = {
    val start = System.currentTimeMillis()
    val iterator = new LuceneDictionary(reader, "status").getEntryIterator()
    val sortedStatuses = toList(iterator).distinct.map(bySatus(_)).sortBy(s => (s.bytes.negate(), -s.status))
    
    val total = sortedStatuses.map(_.bytes).reduceLeft((a,b) => a.add(b))
    println(s"Total trafic: $total bytes")
    sortedStatuses.map(s => s"Status ${s.status} ${"%.6f".format(percentage(s.bytes, total))}% ${s.bytes.setScale(0)}").map(println)
    val time = System.currentTimeMillis() - start
    println(s"Global statuses took: $time ms.")
  }
  
  private def percentage(value: BigDecimal, total: BigDecimal) = {
    value.floatValue() / total.floatValue() * 100
  }
  
  def maximumIp() = {
    val start = System.currentTimeMillis()

    val ld = new LuceneDictionary(reader, "ip")
    val iterator: InputIterator = ld.getEntryIterator()
    var maximus = IpSum("", BigDecimal.valueOf(0), 0)
    var byteRef = iterator.next()
    while (byteRef != null) {
      val term = byteRef.utf8ToString()
      if (term.length() > 0) {
       val maxChalanger = byIp(term) 
       if(maxChalanger.bytes.compareTo(maximus.bytes) > 0)
         maximus = maxChalanger
      }
      byteRef = iterator.next()
    }
    val time = System.currentTimeMillis() - start
    maximus.copy(time = time)
  }
  
  def bySatus(status: Int) = {
     val start = System.currentTimeMillis()
     val sum = 
       try {
          val query = NumericRangeQuery.newIntRange("status", status, status, true, true)
	      val results = searcher.search(query, 5000000)
	      var bigN = BigDecimal.valueOf(0)
	      val byteResults = results.scoreDocs.map{
	        sd => 
	          val doc = searcher.doc(sd.doc)
	          val tempVal = doc.getField("bytes").numericValue().floatValue()
	          bigN = bigN.add(BigDecimal.valueOf(tempVal))
	      }.toList
	      bigN
      } catch {
      case ex: Exception => println(s"Statusul $status nu a putut fi gasit!")
      BigDecimal.valueOf(0)
    }
    val time = System.currentTimeMillis() - start
    StatusSum(status, sum)
  }
  
  def statusSumByIp(ip: String) = {
    val start = System.currentTimeMillis()
    val sums: scala.collection.mutable.Map[Int, BigDecimal] = scala.collection.mutable.Map.empty
    try {
      val query = ipParser.parse(ip)
      val results = searcher.search(query, 5000000)
      
      val byteResults = results.scoreDocs.map{
        sd => 
          val doc = searcher.doc(sd.doc)
          val bytes = doc.getField("bytes").numericValue().floatValue()
          val status = doc.getField("status").numericValue().intValue()
          val oldVal = sums.getOrElse(status, BigDecimal.valueOf(0))
          sums(status) = oldVal.add(BigDecimal.valueOf(bytes))
      }.toList
    } catch {
      case ex: Exception => println(s"Ip-ul $ip nu a putut fi gasit!")
      BigDecimal.valueOf(0)
    }
    val time = System.currentTimeMillis() - start
    println(s"Status search for ip: $ip took $time ms.")
    sums.toList.sortBy(p => p._1).map(p => println(s"status: ${p._1} bytes: ${p._2}"))
  }

  def byIp(ip: String) = {
    val start = System.currentTimeMillis()
    val sum = try {
      val query = ipParser.parse(ip)
      val results = searcher.search(query, 5000000)
      var bigN = BigDecimal.valueOf(0)
      val byteResults = results.scoreDocs.map{
        sd => 
          val doc = searcher.doc(sd.doc)
          val tempVal = doc.getField("bytes").numericValue().floatValue()
//          println(s"${doc.getField("bytes")} -> ${doc.getField("bytes").numericValue} -> ${doc.getField("bytes").numericValue().floatValue()}")
          bigN = bigN.add(BigDecimal.valueOf(tempVal))
      }.toList
      
      val numTotalHits = results.totalHits
//      println(s"Pentru ip-ul $ip am un total de $numTotalHits intrari")
      bigN
    } catch {
      case ex: Exception => println(s"Ip-ul $ip nu a putut fi gasit!")
      BigDecimal.valueOf(0)
    }
    val time = System.currentTimeMillis() - start
    IpSum(ip, sum, time)
  }
  
  case class StatusSum(status: Int, bytes: BigDecimal)
  
  case class IpSum(ip: String, bytes: BigDecimal, time: Long)
  
  private def toList(iter: InputIterator, in: List[Int] = List.empty): List[Int] = {
    val byteRef = iter.next()
    if (byteRef != null) {
      toList(iter, in ++ List(NumericUtils.prefixCodedToInt(byteRef)))      
    } else {
      in
    }
  }
}