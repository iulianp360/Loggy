import java.io.File
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.io.Source
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.util.concurrent.CountDownLatch

object Naivu {
  val workers = 20
  val batchSize = 150
  var files: Iterator[File] = null
  private var src: Option[FileSource] = None

  def main(args: Array[String]) {
	  val inputDir = "c://temp//bkp//input//"
//	  files = new File(inputDir).listFiles.iterator
//	  
//	  val boss = new Shefu("Impartit uniform", workers)
//	  val t = new Thread(boss)
//	  t.start
//	  for(i <- 1 to workers) {
//	    new Thread(new Muncitoru(i, boss)).start()
//	  }
//	   
//	  Thread.sleep(5000)
//	  Agregatoru.ipBytes.clear
	  
	  Agregatoru.ipToFind = "178.154.179.250"
	  val fileList = new File(inputDir).listFiles
	  val bossDedicat = new Shefu("Un worker per file", fileList.size)
	  new Thread(bossDedicat).start
	  fileList.foreach(f => new Thread(new Dedicat(f, bossDedicat)).start)
  }

  def nextChunk(): List[String] = this.synchronized{
    src match {
      case Some(fs) => 
        val chunk = fs.nextChunk(batchSize)
        if(chunk.size < batchSize) {
          fs.close
          if(files.hasNext) {
            src = Some(new FileSource(files.next))
          } else {
            src = None
          }
        }
        if(chunk.isEmpty) nextChunk else chunk
      case None =>
        if (files.hasNext) {
          src.map(_.close)
          src = Some(new FileSource(files.next))
          nextChunk
        } else List.empty
    }
  }

}

class FileSource(file: File) {
//  println(s"Opening file ${file.getName}")
  val fin = new FileInputStream(file)
  val gzis = new GZIPInputStream(fin)
  val gzas = new GZIPInputStream(gzis)
  val source = Source.fromInputStream(gzas)
  val lines = source.getLines

  def nextChunk(size: Int): List[String] = buildChunk(List.empty, lines, size)

  def buildChunk(in: List[String], iter: Iterator[String], limit: Int): List[String] = {
      try {
        if (in.size < limit && iter.hasNext) 
          buildChunk(in ++ List(iter.next), iter, limit)
        else in
      } catch {
        case ex: Exception => 
          println(ex)
          println(in)
          println(limit)
          System.exit(1)
          in
      }
  }

  def close() = {
//    println(s"Closing file ${file.getName} \n")
    source.close
    gzas.close
    gzis.close
    fin.close
  }
}

class Dedicat(file: File, boss: Shefu) extends Runnable {
  def run() = {
//    println(s"Opening file ${file.getName}")
    val fin = new FileInputStream(file)
    val gzis = new GZIPInputStream(fin)
    val gzas = new GZIPInputStream(gzis)
    val source = Source.fromInputStream(gzas)
    val lines = source.getLines

    while(lines.hasNext) Agregatoru.munceste(lines.next)
    
//    println(s"Closing file ${file.getName} \n")
    source.close
    gzas.close
    gzis.close
    fin.close
    
    boss.counter.countDown()
  }
}

class Muncitoru(i: Int, boss: Shefu) extends Runnable {
  def run() = {
    var chunk = Naivu.nextChunk
    while (chunk.size > 0 || Naivu.files.size > 0) {
      chunk.foreach(Agregatoru.munceste)
      chunk = Naivu.nextChunk
    }
    boss.counter.countDown()
//    println(s"Muncitoru $i a terminat.")
  }
}

class Shefu(nume: String, workerTotal: Int) extends Runnable {
  val counter: CountDownLatch = new CountDownLatch(workerTotal)
  private val start = System.currentTimeMillis()
  def run() = {
    while (counter.getCount() > 0) {
      Thread.sleep(30)
    }
//    println(s"Linii parsate ${Agregatoru.linesParsed}")
//    println(s"Ip bytes ${Agregatoru.ipBytes}")
//    println(s"Ip bytes ${Agregatoru.ipStatuses.statuses.map(f => s"${f._1} -> ${f._2.value} ").mkString("\n")}")
    println(s"Global statuses ${Agregatoru.statusesBytes.map(f => s"${f._1} -> ${f._2.value} ").mkString("\n")}")
    println(s"$nume cu $workerTotal workeri a terminat in ${System.currentTimeMillis() - start} ms !")
  }
}

object Agregatoru {
  private val ddd = "\\d{1,3}" // at least 1 but not more than 3 times (possessive)
  private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?" // like `123.456.7.89`
  private val client = "(\\S+)" // '\S' is 'non-whitespace character'
  private val user = "(\\S+)"
  private val dateTime = "(\\[.+?\\])" // like `[21/Jul/2009:02:48:13 -0700]`
  private val request = "\"(.*?)\"" // any number of any character, reluctant
  private val status = "(\\d{3})"
  private val bytes = "(\\S+)" // this can be a "-"
  private val referer = "\"(.*?)\""
  private val agent = "\"(.*?)\""
  private val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
  val p = Pattern.compile(regex)
  
  
  def safeVal(m: Matcher, i: Int): String = Option(m.group(i)).getOrElse("")
  def safeIntVal(m: Matcher, i: Int): Int = Option(m.group(i).toInt).getOrElse(0)
  def safeFloatVal(m: Matcher, i: Int): Float = Option(m.group(i).toFloat).getOrElse(0)
  
  def incrementIPBytes(key: String, value: Float) = {
    this.synchronized{
    	val oldVal = ipBytes.get(key).getOrElse(BigDecimal.valueOf(0))
        ipBytes(key) = oldVal + BigDecimal.valueOf(value)
    }
  }
  

  def munceste(line: String) = {
//    collectStatusesForIp(line)
    
    collectAllStatuses(line)
  }
  
  private def collectStatusesForIp(line: String) = {
    try {
      if (line.contains(ipToFind)) {
        val m = Agregatoru.p.matcher(line)
        if (m.find) {
          val ipVal = Agregatoru.safeVal(m, 1)
          val statusVal = Agregatoru.safeIntVal(m, 6)
          val bytesVal = Agregatoru.safeFloatVal(m, 7)
          if (ipVal == ipToFind) {
            this.synchronized{
              ipStatuses.add(statusVal, bytesVal)
            }
          }
        }
      }
    } catch {
      case ex: Exception => println(s"Eroare $ex la linia $line")
    }
  }
  
  private def collectAllStatuses(line: String) = {
    try {
        val m = Agregatoru.p.matcher(line)
        if (m.find) {
          val ipVal = Agregatoru.safeVal(m, 1)
          val statusVal = Agregatoru.safeIntVal(m, 6)
          val bytesVal = Agregatoru.safeFloatVal(m, 7)
	        this.synchronized {
	          val oldStatusBytes = statusesBytes.get(statusVal).getOrElse(new StatusByteCounter(0))
	          oldStatusBytes.add(bytesVal)
	          statusesBytes.put(statusVal, oldStatusBytes)
	        }
        }
    } catch {
      case ex: Exception => println(s"Eroare $ex la linia $line")
    }
  }
    
  var linesParsed = 0
  var ipBytes: scala.collection.mutable.Map[String, BigDecimal] = scala.collection.mutable.Map.empty
  // 1.1,  1.2
  
  var ipToFind = ""
  var statusesBytes: scala.collection.mutable.Map[Int, StatusByteCounter] =scala.collection.mutable.Map.empty
  var ipStatuses = new IpStatuses("")
}

class IpStatuses(ip: String) {
  val statuses: scala.collection.mutable.Map[Int, StatusByteCounter] = scala.collection.mutable.Map.empty
  
  def add(status: Int, bytes: Float) = {
    statuses.get(status) match {
      case Some(counter) => counter.add(bytes)
      case _ => statuses.put(status, new StatusByteCounter(bytes))
    }
  }
}

class StatusByteCounter(initial: Float = 0) {
  var value = BigDecimal.valueOf(initial)
  
  def add(v: Float) = {
    value = value.+(BigDecimal.valueOf(v))
  }
}