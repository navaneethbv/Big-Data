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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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

public class ReduceSideJoin {
	
	public static class Top10IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text line, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			if(line.getLength()>0)
			{
				String[] fields=line.toString().split("\t");	
				String value= "a:\t"+fields[1];
				context.write(new Text(fields[0].trim()), new Text(value));
			}
		}
		

	}
	
	public static class DetailsMapper extends Mapper<LongWritable, Text, Text, Text> {

		static String total_record = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {

			Text business_id = new Text();
			Text details = new Text();

			total_record = total_record.concat(line.toString());
			String[] fields = total_record.split(Pattern.quote("^"));
			if (fields.length == 3) {
				if (!fields[0].isEmpty()) {
					String full_address=fields[1].trim();
					String categories=fields[2].trim();
					String value="b:\t"+full_address+"\t"+categories;
					
					business_id.set(fields[0].trim());
					details.set(value);
					context.write(business_id, details);
				}
				total_record="";
			}
		}
	}
	
	public static class Top10Join_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text business_id, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			boolean flag = false;

			String dataA_str = "";
			String dataB_str = "";
			for (Text value : values) {
				if (value.toString().contains("a:")) {
					dataA_str = value.toString();
					flag = true;
				} else
					dataB_str = value.toString();
			}
			if (!flag)
				return;

			String[] dataB = dataB_str.split("\t");
			String[] dataA = dataA_str.split("\t");
			String data = dataB[1] + "\t" + dataB[2] + "\t" + dataA[1];
			context.write(business_id, new Text(data));
		}
	}

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
		protected void cleanup(Reducer<Text,FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {

			Map<String, Float> sortedMap = new TreeMap<String, Float>(new ValueComparator(map));
			sortedMap.putAll(map);
			int i = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()),	new FloatWritable(entry.getValue()));
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
		
		if (otherArgs.length != 4) {
			System.err.println("Usage: ReduceSideJoin <in> <in2> <out-intermediate> <out>");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Yelp Top 10 Rated Business");
		
		job.setJarByClass(ReduceSideJoin.class);
		
		Path inputFile = new Path(otherArgs[0]);
		Path inputFile2 = new Path(otherArgs[1]);
		Path outputFile = new Path(otherArgs[3]);
		Path intermediateFile = new Path(otherArgs[2]);
		
		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, intermediateFile);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setMapperClass(Top10_Mapper.class);
		job.setReducerClass(Top10_Reducer.class);
		
		FileInputFormat.setMinInputSplitSize(job, 150000);
		
		job.waitForCompletion(true);
		
		/*
		 * Job 2
		 * */
	
		Job job2 = new Job(conf, "Joiner");
		job2.setJarByClass(ReduceSideJoin.class);

		job2.setReducerClass(Top10Join_Reducer.class);

		MultipleInputs.addInputPath(job2, intermediateFile,
				TextInputFormat.class, Top10IdentityMapper.class);
		MultipleInputs.addInputPath(job2, inputFile2, TextInputFormat.class,
				DetailsMapper.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job2, outputFile);
		FileInputFormat.setMinInputSplitSize(job2, 500000000);

		job2.waitForCompletion(true);
	}
}
