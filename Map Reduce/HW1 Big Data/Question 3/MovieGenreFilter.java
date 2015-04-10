/*******************************************************************************
 * Developed By: Snehal Sutar
 * Net Id: svs130130
 * Description: Q3 List all movie title where genre is “fantasy”. 
 * The genre input must be taken from command line. Use the 
 * movies.dat file.
 ******************************************************************************/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieGenreFilter {
	// Global variable declaration.
	public static String genre_name;

	/***************************************************************************
	 * Mapper CLASS.
	 * 
	 * @author Snehal
	 * 
	 **************************************************************************/
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {

//		public void configure(JobConf job) {
//			genre_name = job.get("genre");
//		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Convert the input "value" from TEXT format to STRING format.
			String in_str = value.toString();

			// Split the line which we get in the format of
			// MovieID::Title::Genres
			String[] split_str = in_str.split("::");

			// Copy the values in respective fields.
			String title = split_str[1];
			String genre_list = split_str[2];
			// Next split the genre list into individual genres.
			String genre[] = genre_list.split("\\|");

			System.out.println(genre_name);
			Configuration conf = context.getConfiguration();
			genre_name = conf.get("genre");
			for (int i = 0; i < genre.length; i++) {
				if (genre[i].equals(genre_name)) {
					context.write(new Text(title), NullWritable.get());
				}
			}
		}
	}

	/***************************************************************************
	 * Main method.
	 * 
	 * @param args
	 * @throws Exception
	 **************************************************************************/
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}
		
		conf.set("genre", args[2]);

		// Copy the genre name into global variable.
//		JobConf jobConf = new JobConf(MovieGenreFilter.class);
//		jobConf.set("genre", args[2]);
//		genre_name = args[2].toString();
//		genre_name.replaceAll(" ", "");
//		System.out.println(genre_name);

		// create a job with name "MovieGenreFilter"
//		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "moviegenrefilter");
		job.setJarByClass(MovieGenreFilter.class);
		job.setMapperClass(Map.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
