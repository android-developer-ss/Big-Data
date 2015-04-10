/*******************************************************************************
 * Developed By: Snehal Sutar
 * Net Id: svs130130
 * Subject: Big Data
 * ****************************************************************************/

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part2 {
	/***************************************************************************
	 * MAPPER 1
	 * 
	 * @author Snehal
	 * 
	 **************************************************************************/
	// The Mapper classes and reducer code
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		// Local variable declaration.
		private String str_mymovieid;
		HashMap<String, String> myMap;
		private Text text_rating;
		private Text text_userid = new Text(); // type of output key

		// Read // Users.dat UserID::Gender::Age::Occupation::Zip-code
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			myMap = new HashMap<String, String>();
			Configuration conf = context.getConfiguration();
			str_mymovieid = conf.get("movieid"); // for retrieving data you set
													// in
			// driver code
			Path[] localPaths = context.getLocalCacheFiles();
			for (Path myfile : localPaths) {
				String line = null;
				String nameofFile = myfile.getName();
				File file = new File(nameofFile + "");
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				line = br.readLine();
				while (line != null) {
					String[] arr = line.split("::");
					myMap.put(arr[0], arr[1] +" "+ arr[2]); // userid(key): gender & age
					line = br.readLine();
				}
				br.close();
			}
		}

		// ----------------------------------------------------------------------
		// Read rating.dat file -- UserID::MovieID::Rating::Timestamp
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			int int_rating = Integer.parseInt(mydata[2]);
			// / System.out.println(value.toString());
			String intrating = mydata[2];
			if (int_rating >= 4 && str_mymovieid.equals(mydata[1])) {
				text_rating = new Text("rat~" + intrating);
				text_userid.set(mydata[0].trim() +" "+ myMap.get(mydata[0]));
				context.write(text_userid, text_rating);
			}
		}
	}

	/***************************************************************************
	 * MAIN CLASS
	 * 
	 * @author Snehal
	 * 
	 **************************************************************************/

	// Driver code
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: JoinExample <in> <in2> <out> <anymovieid>");
			System.err
					.println("Enter: hadoop jar part2.jar Part2 rating output 1 ");
			System.exit(2);
		}

		conf.set("movieid", otherArgs[2]); // setting global data variable for
											// hadoop

		// create a job with name "joinexc"
		Job job = new Job(conf, "joinexc");
		job.setJarByClass(Part2.class);

		// job.setReducerClass(Reduce.class);

		// OPTIONAL :: uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);

		// rating.dat
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, Map1.class);

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		job.addCacheFile(new URI(NAME_NODE + "/user/hue/users/users.dat"));

		// users.dat
		// MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
		// TextInputFormat.class, Map2.class);

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
	}
}
