import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Drive {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "TWITTER");
		job.setJarByClass(Drive.class);
		job.setMapperClass(MyMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("twitter", MyReduce.class, job);
		job.setReducerClass(MyReduce.class);
		job.waitForCompletion(true);
	}

	public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private final String delimit = Character.toString((char) 31);

		enum Month {
			Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			if (line.length() == 0) {
				return;
			}
			JsonParser jsonParser = new JsonParser();
			JsonObject twitterJson = jsonParser.parse(line).getAsJsonObject();
			String created_at = twitterJson.get("created_at").getAsString();
			String time = parseTime(created_at);

			String tid = twitterJson.get("id").getAsString();

			String text = twitterJson.get("text").getAsString();

			outputKey.set(time);
			outputValue.set(tid + delimit + text);
			context.write(outputKey, outputValue);

		}

		// ==================helper function================

		private static String parseTime(String created_at) {
			String[] items = created_at.split("\\s+");
			String year = items[5];
			String month = String.format("%02d", (Month.valueOf(items[1])
					.ordinal() + 1));
			String day = items[2];
			String time = items[3];
			return year + "-" + month + "-" + day + "+" + time;
		}

	}

	public static class MyReduce extends
			TableReducer<Text, Text, ImmutableBytesWritable> {
		public static final byte[] TF = "twitterfamily".getBytes();
		public static final byte[] CREATED_AT = "created_at".getBytes();
		public static final byte[] TEXT = "text".getBytes();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				String[] items = val.toString().split(
						Character.toString((char) 31));
				String tid = items[0];
				String text = items[1];
				Put put = new Put(Bytes.toBytes(tid));
				put.add(TF, CREATED_AT, Bytes.toBytes(key.toString()));
				put.add(TF, TEXT, Bytes.toBytes(text));
				context.write(null, put);
			}

		}
	}

}
