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

object Cautatoru {

  val indexFolder = "c://temp//bkp//index_bkp//"
  val reader = DirectoryReader.open(FSDirectory.open(new File(indexFolder)))

  val searcher = new IndexSearcher(reader)
  val analyzer = new StandardAnalyzer(Version.LUCENE_4_10_2)
  val parser = new QueryParser(Version.LUCENE_4_10_2, "ip", analyzer)

  def main(args: Array[String]): Unit = {
//    display(maximumIp)

    display(byIp("178.154.179.250"))
    
    display(byIp("213.175.86.38"))
    
    display(byIp("66.249.74.147"))
  }
  
  def display(ip: IpSum) = println(s"Ip ${ip.ip} has ${ip.bytes} bytes found in ${ip.time} ms")
  
  def maximumIp() = {
    val start = System.currentTimeMillis()

    val ld = new LuceneDictionary(reader, "ip")
    val iterator: InputIterator = ld.getEntryIterator()
    var maximus = IpSum("", 0, 0)
    var byteRef = iterator.next()
    while (byteRef != null) {
      val term = byteRef.utf8ToString()
      if (term.length() > 0) {
       val maxChalanger = byIp(term) 
       if(maxChalanger.bytes > maximus.bytes)
         maximus = maxChalanger
      }
      byteRef = iterator.next()
    }
    val time = System.currentTimeMillis() - start
    maximus.copy(time = time)
  }

  def byIp(ip: String) = {
    val start = System.currentTimeMillis()
    val sum = try {
      val query = parser.parse(ip)
      val results = searcher.search(query, 5000000)
      var bigN = BigDecimal.valueOf(0)
      val byteResults = results.scoreDocs.map{
        sd => 
          val doc = searcher.doc(sd.doc)
          val tempVal = doc.getField("bytes").numericValue().floatValue()
          
          bigN = bigN.+(BigDecimal.valueOf(tempVal))
      }.toList
      
//      val numTotalHits = results.totalHits
//      System.out.println(s"Pentru ip-ul $ip am un total de $numTotalHits intrari si ${byteResults.sum} bytes")
      bigN
    } catch {
      case ex: Exception => println(s"Ip-ul $ip nu a putut fi gasit!")
      BigDecimal.valueOf(0)
    }
    val time = System.currentTimeMillis() - start
    IpSum(ip, sum, time)
  }
  
  case class IpSum(ip: String, bytes: BigDecimal, time: Long)
}