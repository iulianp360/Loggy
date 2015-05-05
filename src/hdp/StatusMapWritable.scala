import java.io.DataOutput
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable
import java.io.DataInput
import org.apache.hadoop.io.Text

class StatusMapWritable(total: LongWritable, statuses: MapWritable) extends Writable {
  
  def this() = this(new LongWritable(0), new MapWritable)
  
  def getTotal = total
  def getStatuses = statuses
  
  def addStatus(status: Text, bytes: LongWritable) = {
    total.set(total.get + bytes.get)
    val sum = 
      if(statuses.containsKey(new Text(status.toString())))
       statuses.get(new Text(status.toString())).asInstanceOf[LongWritable].get + bytes.get
      else
        bytes.get
        
    statuses.put(new Text(status.toString()), new LongWritable(sum))  
  }

  @Override
  def readFields(in: DataInput) = {
    total.readFields(in);
    statuses.readFields(in);
  }

  @Override
  def write(out: DataOutput) = {
    total.write(out);
    statuses.write(out);
  }
  
  @Override
  override def toString() = {
    statuses.entrySet().toArray().mkString("\n") + " from a total of " + total.get 
  }
} 
