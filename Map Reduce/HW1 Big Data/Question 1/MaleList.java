/********************************************************************
 * Developed By: Snehal Sutar
 * Net Id: svs130130
 * Description: Q1 list all male user id whose age is less or equal 
 * to 7. 
 * Using the users.dat file, list all the male userid filtering by 
 * age. This demonstrates the use of mapreduce to filter data.
 ********************************************************************/

import java.io.IOException;

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

public class MaleList {

	/***************************************************************************
	 * Mapper CLASS.
	 * @author Snehal
	 *
	 **************************************************************************/
	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String in_str = value.toString();

			// Split the line which we get in the format of
			// UserID::Gender::Age::Occupation::Zip-code
			String[] split_str = in_str.split("::");

			// Copy the values in respective fields.
			Text user_id  = new Text(split_str[0]);
			String gender = split_str[1];
			int age 	  = Integer.parseInt(split_str[2]);

			if (gender.equals("M") && age <= 7) {
				context.write(user_id, NullWritable.get());
			}
		}
	}
	
	/***************************************************************************
	 * Main method.
	 * @param args
	 * @throws Exception
	 **************************************************************************/
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}
		// create a job with name "male list less than 7 years"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "malelist");
		job.setJarByClass(MaleList.class);
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
