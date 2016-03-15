import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.HashMap; 
import java.util.Map; 
import java.util.regex.Pattern; 

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class TreeMapComparator implements Comparator<Object> {
	 
	Map<String, Float> map;
 
	public TreeMapComparator(Map<String, Float> map) {
		this.map = map;
	}
	public int compare(Object keyA, Object keyB) {
		
		Float valueA= (Float) map.get(keyA);
		Float valueB= (Float) map.get(keyB);
		
		int compare=valueB.compareTo(valueA);
				
		return (compare==0)?1:compare;
	}
}

public class MapSideJoin {
	
	public static class MapjoinMapper extends Mapper<LongWritable, Text, Text, Text> {

		static HashMap<String, String> map = new HashMap<String, String>();

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			//myCenterList = new ArrayList<>();
			Configuration conf = context.getConfiguration();
			String myfilepath = "/user/nbv140130/dft-output4i/part-r-00000";
			Path part=new Path("hdfs://cshadoop1"+myfilepath);//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();	        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		            //System.out.println(line);
		            //do what you want with the line read
		            String[] fields = line.split("\t");
					map.put(fields[0].trim(), fields[1].trim());
					line = br.readLine();
		            line=br.readLine();
		        }
		       
		    }
		}
		
	/*	@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			URI[] files = context.getCacheFiles();

			if (files.length == 0) {
				throw new FileNotFoundException("Distributed cache file not found");
			}
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(new Path(files[0]));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			readCacheFile(br);
		};

		private void readCacheFile(BufferedReader br) throws IOException {
			String line = br.readLine();
			while (line != null) {
				String[] fields = line.split("\t");
				map.put(fields[0].trim(), fields[1].trim());
				line = br.readLine();
			}
		}
	*/
		@Override
		protected void map(LongWritable key, Text line,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Text user_id = new Text();
			Text review_text = new Text();

			String[] fields = line.toString().split(Pattern.quote("^"));
			if (fields.length == 4) {
				String userid = fields[1].trim();
				String reviewStars = fields[3].trim();
				String business_id = fields[2].trim();

				String city = map.get(business_id);

				if (city != null) {
					if (city.contains("Stanford,")) {
						review_text.set(reviewStars);
						user_id.set(userid);
						context.write(user_id, review_text);
					}
				}
			}
		}
	}


	public static class BusinessMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
			throws IOException, InterruptedException {
		
			Text business_id = new Text();
			Text address = new Text();
			String[] fields = line.toString().split(Pattern.quote("^"));
			if (fields.length == 3) {
				business_id.set(fields[0].trim());
				address.set(fields[1].trim());
				context.write(business_id, address);
			}
		}
	}

	public static class BusinessReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text business_id, Iterable<Text> address,
				Context context) throws IOException, InterruptedException {

			for (Text add : address) {
				context.write(business_id, add);
			}

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		Configuration conf=new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(otherArgs.length!=4)
		{
			System.err.println("Incompatible Number Of Arguments");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job1=new Job(conf,"Filter Business Entries");
		
		job1.setJarByClass(MapSideJoin.class);

		Path inputFile=new Path(otherArgs[0]);
		Path inputFile2=new Path(otherArgs[1]);
		Path intermediateFile=new Path(otherArgs[2]);
		Path outputFile=new Path(otherArgs[3]);
				
		FileInputFormat.addInputPath(job1, inputFile);
		FileOutputFormat.setOutputPath(job1, intermediateFile);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setMapperClass(BusinessMapper.class);
		job1.setReducerClass(BusinessReducer.class);
		
		FileInputFormat.setMinInputSplitSize(job1, 150000);
		
		job1.waitForCompletion(true);
		
//-----------------------------------------------------------------------------------------
		
		@SuppressWarnings("deprecation")
		Job job2=new Job(conf,"Joiner");
		job2.setJarByClass(MapSideJoin.class);

		job2.setMapperClass(MapjoinMapper.class);
		
		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		//job2.addCacheFile(new URI(intermediateFile.getName()+"/part-r-00000"));
		
		FileInputFormat.addInputPath(job2, inputFile2);
		FileOutputFormat.setOutputPath(job2, outputFile);
		FileInputFormat.setMinInputSplitSize(job2, 150000);
		
		job2.waitForCompletion(true);
		
	}
}
