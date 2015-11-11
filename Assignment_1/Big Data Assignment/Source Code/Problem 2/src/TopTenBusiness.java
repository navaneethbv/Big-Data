import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class ValueComparator implements Comparator<Object> {
	 
	Map<String, Float> map;
 
	public ValueComparator(Map<String, Float> map) {
		this.map = map;
	}
	public int compare(Object keyA, Object keyB) {
		
		Float valueA= (Float) map.get(keyA);
		Float valueB= (Float) map.get(keyB);
		
		int compare=valueB.compareTo(valueA);
		
		if(compare==0)
			return 1;		
		return compare;
	}
}

public class TopTenBusiness {

	public static class Top10_Mapper extends
		Mapper<LongWritable, Text, Text, FloatWritable> {
		
		static String total_record = "";
		
		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {
		
			Text business_id = new Text();
			FloatWritable stars = new FloatWritable(1);
		
			total_record = total_record.concat(line.toString());
			String[] fields = total_record.split(Pattern.quote("^"));
			if (fields.length == 4) {
				if (!fields[3].isEmpty()) {
					business_id.set(fields[2].trim());
					stars.set(Float.parseFloat(fields[3].trim()));
					context.write(business_id, stars);
				}
				total_record = "";
			}
		}
	}

	public static class Top10_Reducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		HashMap<String, Float> map = new HashMap<String, Float>();

		@Override
		protected void reduce(Text business_id, Iterable<FloatWritable> stars,
				Context context) throws IOException, InterruptedException {

			FloatWritable average = new FloatWritable(0);
			int total = 0;
			int count = 0;
			for (FloatWritable star : stars) {
				total += star.get();
				count++;
			}

			float avg = total / count;
			average.set(avg);
			map.put(business_id.toString(), avg);
		}

		@Override
		protected void cleanup(
				Reducer<Text,FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {

			Map<String, Float> sortedMap = new TreeMap<String, Float>(
					new ValueComparator(map));
			sortedMap.putAll(map);
			int i = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()),
						new FloatWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
		}
	}
	
	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException {
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTenBusiness <in> <out>");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Yelp Top 10 Rated Business");
		
		job.setJarByClass(TopTenBusiness.class);
		
		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);
		
		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setMapperClass(Top10_Mapper.class);
		job.setReducerClass(Top10_Reducer.class);
		
		FileInputFormat.setMinInputSplitSize(job, 150000);
		
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	
	}
}