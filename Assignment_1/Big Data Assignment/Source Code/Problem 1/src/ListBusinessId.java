import java.io.IOException;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ListBusinessId {
	public static class PaloAltoFilterMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {

			Text business_id = new Text();
			String lineInput = line.toString();
			String[] fields = lineInput.split(Pattern.quote("^"));
			if (fields.length == 3) {
				if (fields[1].contains("Palo Alto")) {
					business_id.set(fields[0].trim());
					NullWritable nullOb = NullWritable.get();
					context.write(business_id, nullOb);
				}
				
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: ListBusinessId <in> <out>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Palo Alto Count");

		job.setJarByClass(ListBusinessId.class);

		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);

		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(PaloAltoFilterMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setMinInputSplitSize(job, 500000000);

		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
