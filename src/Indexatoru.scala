import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.LongField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version
import java.io.File
import scala.io.Source
import java.util.regex.Pattern
import org.apache.lucene.document.IntField
import org.apache.lucene.document.FloatField
import java.util.regex.Matcher
import java.util.zip.GZIPInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import scala.actors.threadpool.FutureTask
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.apache.lucene.store.RAMDirectory

object Indexatoru {

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
  private val p = Pattern.compile(regex)

  def main(args: Array[String]): Unit = {
    val inputFolder = "c://temp//bkp//input//"
    val indexFolder = "c://temp//bkp//index//" // fielduri detaliate

    indexeazaFisiere(inputFolder, indexFolder)
  }

  def indexeazaFisiere(srcDir: String, indexDir: String) = {
    val inputDir = new File(srcDir)
    if (inputDir.isDirectory()) {
      inputDir
      .listFiles
      .foreach(indexAndSave(_, indexDir))
    }  
  }

  def indexAndSave(f: File, indexDir: String) = {
    try {
      val analyzer = new StandardAnalyzer(Version.LUCENE_4_10_2)
      val iwc = new IndexWriterConfig(Version.LUCENE_4_10_2, analyzer)
      iwc.setOpenMode(OpenMode.CREATE_OR_APPEND)
      val dir = new RAMDirectory()
      val writer = new IndexWriter(dir, iwc)
      // Lucene magic
      indexFile(f, writer)
      writer.close()

      // Merge ram dir into the file system dir
      save(dir, indexDir)
    } catch {
      case ex: Exception => println(s"Bubuuu $ex")
    }
  }

  def save(ram: RAMDirectory, indexDir: String) {
    print("Salvez ramdir pe disk")
    val analyzer = new StandardAnalyzer(Version.LUCENE_4_10_2)
    val iwc = new IndexWriterConfig(Version.LUCENE_4_10_2, analyzer)
    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND)
    val fs = FSDirectory.open(new File(indexDir))
    val diskWriter = new IndexWriter(fs, iwc)
    try {
      diskWriter.addIndexes(ram)
    } finally {
      diskWriter.close()
    }
    println("... done")
  } 

  def indexFile(file: File, writer: IndexWriter) = {
    val start = System.currentTimeMillis()
    println(s"Indexez fisierul ${file.getName}")
    val fin = new FileInputStream(file)
    val gzis = new GZIPInputStream(fin)
    val gzas = new GZIPInputStream(gzis)
    val source = Source.fromInputStream(gzas)
    for (line <- source.getLines) {
      addLineAsDoc(line, writer)
    }
    source.close
    gzas.close
    gzis.close
    fin.close
    println(s" ... a durat ${System.currentTimeMillis() - start} ms.")
  }

  
  def addLineAsDoc(line: String, writer: IndexWriter) = {
    try {
      val m = p.matcher(line)
      if (m.find) {
        val document = new Document()
        document.add(new StringField("ip", safeVal(m, 1), Field.Store.YES))
        document.add(new StringField("client", safeVal(m, 2), Field.Store.YES))
        document.add(new StringField("user", safeVal(m, 3), Field.Store.YES))
        document.add(new StringField("dateTime", safeVal(m, 4), Field.Store.YES))
        document.add(new StringField("request", safeVal(m, 5), Field.Store.YES))
        document.add(new IntField("status",safeIntVal(m, 6), Field.Store.YES))
        document.add(new FloatField("bytes", safeFloatVal(m, 7), Field.Store.YES))
        document.add(new StringField("referer", safeVal(m, 8), Field.Store.YES))
        document.add(new StringField("agent", safeVal(m, 9), Field.Store.YES))

        writer.addDocument(document)
        writer.commit()
      }
    } catch {
      case ex: Exception => println(s"Eroare $ex")
    }
  }
  
  private def safeVal(m: Matcher, i: Int): String = Option(m.group(i)).getOrElse("")
  private def safeIntVal(m: Matcher, i: Int): Int = Option(m.group(i).toInt).getOrElse(0)
  private def safeFloatVal(m: Matcher, i: Int): Float = Option(m.group(i).toFloat).getOrElse(0)

}