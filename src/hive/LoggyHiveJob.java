import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LoggyHiveJob {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.out.println("Usage: LoggyHiveJob <in> <out>");
		} else {
			Job job = new Job(conf);
			job.setJarByClass(LoggyHiveMapper.class);
			job.setMapperClass(LoggyHiveMapper.class);
			job.setCombinerClass(LoggyHiveReducer.class);
			job.setReducerClass(LoggyHiveReducer.class);
			job.setOutputKeyClass(BytesWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path((args[1])));
			job.waitForCompletion(true);

		}

	}

}

class LoggyHiveMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	public static final String SEPARATOR_FIELD = new String(new char[] {1}); 
	public static final BytesWritable NULL_KEY = new BytesWritable(null);
	
    private String ip = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})?"; // like `123.456.7.89`
	private String client = "(\\S+)"; // '\S' is 'non-whitespace character'
	private String user = "(\\S+)";
	private String dateTime = "(\\[.+?\\])"; // like `[21/Jul/2009:02:48:13-0700]`
	private String request = "\"(.*?)\""; // any number of any character, reluctant
	private String status = "(\\d{3})";
	private String bytes = "(\\S+)"; // this can be a "-"
	private String referer = "\"(.*?)\"";
	private String agent = "\"(.*?)\"";
	private String regex = ip+" "+client+" "+user+" "+dateTime+" "+request+" "+status+" "+bytes+" "+referer+" "+agent;
	Pattern p = Pattern.compile(regex);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Matcher m = p.matcher(value.toString());
		if (m.find()) {
			StringBuilder hiveRow = new StringBuilder();
			hiveRow.append(m.group(1));
			hiveRow.append(SEPARATOR_FIELD);
			hiveRow.append(m.group(2));
			hiveRow.append(SEPARATOR_FIELD);
			hiveRow.append(m.group(3));
			hiveRow.append(SEPARATOR_FIELD);
			hiveRow.append(m.group(4));
			hiveRow.append(SEPARATOR_FIELD);
			hiveRow.append(m.group(5));
			hiveRow.append(SEPARATOR_FIELD);
			hiveRow.append(m.group(6));
			hiveRow.append(SEPARATOR_FIELD);
			hiveRow.append(m.group(8));
			hiveRow.append(SEPARATOR_FIELD);
			hiveRow.append(m.group(9));

			long l = 0;
			try {
				l = Long.parseLong(m.group(7));
			} catch (Exception e) {
				// do nothing
			}
			context.write(new Text(hiveRow.toString()), new LongWritable(l));
		}

	}
}


class LoggyHiveReducer extends Reducer<Text, IntWritable, BytesWritable, Text> {

public static final String SEPARATOR_FIELD = new String(new char[] {1}); 
public static final BytesWritable NULL_KEY = new BytesWritable(null);

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	    float sum = 0;
	    Iterator<IntWritable> iter = values.iterator();
	    while(iter.hasNext()) {
	    	IntWritable val = iter.next();
	    	sum += val.get();
	    }
	    context.write(NULL_KEY, new Text(key.toString() + SEPARATOR_FIELD + sum));
	}
}