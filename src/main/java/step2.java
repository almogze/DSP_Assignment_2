<<<<<<< HEAD
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class step2 {
	/**
	 * The Input:
	 *      Google 2gram database
	 *              n-gram /T year /T occurrences /T pages /T books
	 *              program is     1991    3   2   1
	 *
	 * The Output:
	 *      For each line from the input it creates a line with the word and its occurrences.
	 *               T n-gram /T occurrences
	 *               program  is		3  
	 *               program  is        1  
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String[] words = strings[0].split(" ");
			if(words.length>1){
				String w1 = words[0];
				String w2 = words[1];
				int occur = Integer.parseInt(strings[2]);
				Text text = new Text();
				text.set(String.format("%s %s",w1,w2));
				Text text1 = new Text();
				text1.set(String.format("%d",occur));
				context.write(text ,text1);
			}
		}

	}


	/**
	 * Input:
	 *      The input is the sorted output of the mapper 
	 *      Maybe output from different mappers.
	 *      Template:
	 *               T n-gram /T occurrences
	 *                 program  is		3  
	 *                 program  is      1 
	 *
	 * Output:
	 *      Combines all the occurrences with the same key.
	 *               T n-gram   	T occurrences
	 *               program is     	4   
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String oldKey=key.toString();
			int sum_occ = 0;
			for (Text val : values) {
				sum_occ += Long.parseLong(val.toString());
			}
			Text newKey = new Text();
			newKey.set(String.format("%s",oldKey));
			Text newVal = new Text();
			newVal.set(String.format("%d",sum_occ));
			context.write(newKey, newVal);

		}
	}

	private static class myPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "2gram");
		job.setJarByClass(step2.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		String output="/output2/";
		SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);


	}

}
=======
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class step2 {
	/**
	 * Input to the mapper:
	 * Key: Line number (not important).
	 * Value: (2-gram - the actual words,
	 *        year of this aggregation,
	 *        occurences in this year,
	 *        pages - The number of pages this 2-gram appeared on in this year,
	 *        books - The number of books this 2-gram appeared in during this year)
	 *
	 * Output of Mapper:
	 *        Key: The word.
	 *        Value: The amount of times it appeares in the year of this record.
	 *
	 * Input of Reducer:
	 *        Output of mapper.
	 *
	 * Output of Reducer:
	 *        Key: a pair of words (sepparated by a whitespace).
	 *        Value: The total amount of times it appears in the corpus.
	 *
	 *        Notice that this is practically word-count.
	 *
	 * Example input:
	 *
	 * Example output:
	 *
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String[] words = strings[0].split(" ");
			if(words.length>1){
				String w1 = words[0];
				String w2 = words[1];
				int occur = Integer.parseInt(strings[2]);
				Text text = new Text();
				text.set(String.format("%s %s",w1,w2));
				Text text1 = new Text();
				text1.set(String.format("%d",occur));
				context.write(text ,text1);
			}
		}

	}


	/**
	 * Input:
	 *      The input is the sorted output of the mapper 
	 *      Maybe output from different mappers.
	 *      Template:
	 *               T n-gram /T occurrences
	 *                 program  is		3  
	 *                 program  is      1 
	 *
	 * Output:
	 *      Combines all the occurrences with the same key.
	 *               T n-gram   	T occurrences
	 *               program is     	4   
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String oldKey=key.toString();
			int sum_occ = 0;
			for (Text val : values) {
				sum_occ += Long.parseLong(val.toString());
			}
			Text newKey = new Text();
			newKey.set(String.format("%s",oldKey));
			Text newVal = new Text();
			newVal.set(String.format("%d",sum_occ));
			context.write(newKey, newVal);

		}
	}

	private static class myPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "2gram");
		job.setJarByClass(step2.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		String output="/output2/";
		SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);


	}

}
>>>>>>> 9f5e23333d29b6cbfc85bed57721d2984108ff89
