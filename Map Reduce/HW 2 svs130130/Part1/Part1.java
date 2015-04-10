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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part1 {

	/***************************************************************************
	 * MAPPER 1
	 * 
	 * @author Snehal
	 * 
	 **************************************************************************/
	// The Mapper classes and reducer code
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		String mymovieid;
		HashMap<String, String> myMap;
		private Text text_rating;
		private Text text_movieid = new Text(); // type of output key
		//----------------------------------------------------------------------
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);
			myMap = new HashMap<String, String>();
			// Configuration conf = context.getConfiguration();
			// movieid = conf.get("movieid"); //for retrieving data you set in
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
					if (arr[1].equals("F")) {
						myMap.put(arr[0], arr[1]); // userid and gender
					}
					line = br.readLine();
				}
				br.close();
			}
		}

		//----------------------------------------------------------------------
		// Read RATINGS.dat and join with users.dat
		// Ratings.dat UserID::MovieID::Rating::Timestamp
		// Users.dat UserID::Gender::Age::Occupation::Zip-code
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Local data declaration.
			String[] mydata = value.toString().split("::");
			String rat_userid = mydata[0];
			String rat_movieid = mydata[1];
			String rat_rating = mydata[2];
			// ------------------------------------------------------------------
			if (myMap.containsKey(rat_userid)) {
				text_rating = new Text("rat~" + rat_rating);
				text_movieid.set(rat_movieid.trim());
				context.write(text_movieid, text_rating);
			}
		}
	}

	/***************************************************************************
	 * MAPPER 2
	 * 
	 * @author Snehal
	 * 
	 **************************************************************************/

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private Text text_myTitle = new Text();
		private Text text_movieid = new Text(); // type of output key

		// Read Movies.dat MovieID::Title::Genres
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			System.out.println(value.toString());
			String title = mydata[1];
			text_myTitle.set("mov~" + title);
			text_movieid.set(mydata[0].trim());
			context.write(text_movieid, text_myTitle);
		}
	}

	/***************************************************************************
	 * REDUCER 1
	 * 
	 * @author Snehal
	 * 
	 **************************************************************************/
	// The reducer class
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text text_mov_title = new Text();
		private Text text_avg_rating = new Text();
		private Text text_max_rating_title = new Text();
		private boolean rat_present, mov_present;
		private String str_movie_title;
		private TreeMap<String, Integer> hm_avgratings = new TreeMap<String, Integer>();

		// note you can create a list here to store the values

		// ----------------------------------------------------------------------
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			rat_present = false;
			mov_present = false;
			int count = 0;
			int rating_total = 0;
			for (Text val : values) {
				if (val.toString().contains("rat~")) {
					rat_present = true;
					rating_total = rating_total
							+ Integer.parseInt(val.toString().substring(4)
									.trim());
					count++;
				} else if (val.toString().contains("mov~")) {
					mov_present = true;
					str_movie_title = val.toString().substring(4);
					text_mov_title.set(str_movie_title);
				}
			}// end for
			if (rat_present && mov_present) {
				int avg_rating = rating_total / count;
				hm_avgratings.put(str_movie_title, avg_rating);
			}
		}

		// ---------------------------------------------------------------------
		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			int count = 0;
			Map<String, Integer> map_avgratings_sorted = sortByComparator(
					hm_avgratings, false);
			for (Map.Entry<String, Integer> entry : map_avgratings_sorted
					.entrySet()) {
				count++;
				text_max_rating_title.set(entry.getKey());
				text_avg_rating.set(entry.getValue() + "");
				context.write(text_max_rating_title, text_avg_rating);
				if (count >= 5) {
					break;
				}
			}
		}

		//----------------------------------------------------------------------
		private static Map<String, Integer> sortByComparator(
				Map<String, Integer> unsortMap, final boolean order) {

			List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(
					unsortMap.entrySet());
			// Sorting the list based on values
			Collections.sort(list,
					new Comparator<Map.Entry<String, Integer>>() {
						public int compare(Map.Entry<String, Integer> o1,
								Map.Entry<String, Integer> o2) {
							if (order) {
								return o1.getValue().compareTo(o2.getValue());
							} else {
								return o2.getValue().compareTo(o1.getValue());

							}
						}
					});

			// Maintaining insertion order with the help of LinkedList
			Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
			for (Map.Entry<String, Integer> entry : list) {
				sortedMap.put(entry.getKey(), entry.getValue());
			}

			return sortedMap;
		}
	}

	/***************************************************************************
	 * MAIN METHOD
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
					.println("Use: hadoop jar part1.jar Part1 ratings movies output ");
			System.exit(2);
		}

		// conf.set("movieid", otherArgs[3]); // setting global data variable
		// for
		// hadoop

		// create a job with name "joinexc"
		Job job = new Job(conf, "joinexc");
		job.setJarByClass(Part1.class);

		job.setReducerClass(Reduce.class);

		// OPTIONAL :: uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		job.addCacheFile(new URI(NAME_NODE + "/user/hue/users/users.dat"));

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, Map1.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, Map2.class);

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
	}
}
