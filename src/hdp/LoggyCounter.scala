import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.io.Writable
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.math.BigDecimal
import org.apache.hadoop.io.LongWritable
import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io.MapWritable

class LogMapper extends Mapper[Object, Text, Text, StatusMapWritable] {
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
  
  private def field(m: Matcher, i: Int) = Option(m.group(i)).getOrElse("").trim

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, StatusMapWritable]#Context) = {
    val ipToFind = context.getConfiguration.get("ipToFind")
    if (ipToFind.equals("all")|| value.toString().startsWith(ipToFind)) {
      val m = p.matcher(value.toString)
      if (m.find) {
        val stat = new Text(field(m, 6))
        val total = new LongWritable((field(m, 7).toLong))
        val map = new MapWritable()
        map.put(stat, total)
        context.write(new Text(ipToFind), new StatusMapWritable(total, map))
      }
    }
  }
}

class LogReducer extends Reducer[Text, StatusMapWritable, Text, StatusMapWritable] {
  
  val statuses = new StatusMapWritable(new LongWritable(0), new MapWritable)
  
  override def reduce(key: Text, values: java.lang.Iterable[StatusMapWritable], context: Reducer[Text, StatusMapWritable, Text, StatusMapWritable]#Context) = {
    val ipToFind = context.getConfiguration.get("ipToFind")
    val sum = new LongWritable(0)
    val iter = values.iterator()
    while(iter.hasNext()) {
      val entryIterator = iter.next.asInstanceOf[StatusMapWritable].getStatuses.entrySet().iterator()
      while(entryIterator.hasNext()) {
        val entry = entryIterator.next
        statuses.addStatus(entry.getKey.asInstanceOf[Text], entry.getValue.asInstanceOf[LongWritable])
      }
    }
    context.write(new Text(ipToFind), statuses)
  }
}


object LoggyCounter {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
//    conf.set("ipToFind", "61.161.139.73")
     conf.set("ipToFind", "all")
    
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: wordcount <in> <out>")
    } else {
      val job = new Job(conf, "ip count")
      job.setJarByClass(classOf[LogMapper])
      job.setMapperClass(classOf[LogMapper])
      job.setCombinerClass(classOf[LogReducer])
      job.setReducerClass(classOf[LogReducer])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[StatusMapWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path((args(1))))
      job.waitForCompletion(true)

      
    }
  }
}