import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class step4 {
	/**
	 * Input to the mapper:
	 *       Key: The output of step 2 OR the output of step 3.
	 *       	  Meaning that the key is a series of 2 or 3 words.
	 *       Value: The total amount of appearences of these series in the corpus.
	 *
	 * Output of Mapper:
	 *       Key:
	 *       Value:
	 *
	 * Example input:
	 *
	 * Example output:
	 *
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] words = key.toString().split(" ");
			String w1 = words[0];
			String w2 = words[1];   

			int occ = Integer.parseInt(value.toString()) ;
			Text occurs = new Text();
			occurs.set(String.format("%d",occ));

			Text firstTwo = new Text();
			firstTwo.set(String.format("%s %s",w1,w2));

			if(words.length>2){
				String w3 = words[2];
				Text t = new Text();
				t.set(String.format("%s %s %s %d",w1,w2,w3,occ));
				Text lastTwo = new Text();
				lastTwo.set(String.format("%s %s",w2,w3));
				context.write(firstTwo, t);
				context.write(lastTwo, t);
			}
			else{
			context.write(firstTwo ,occurs);
			}
		}
	}

	/**
	 * Input:
	 *        Output of mapper.
	 *
	 * Output:
	 *        Key:
	 *        Value:
	 *
	 * Example input:
	 *
	 * Example output:
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// We wish to save a local ArrayList<String> that will hold the values received in the reduce procedure,
			// until we find the amount of occurences of "w1 w2" in the corpus.

			List<String> vals = new ArrayList<String>();

			String[] words = key.toString().split(" ");
			String w1 = words[0];
			String w2 = words[1];

			Text newKey = new Text();
			Text newVal = new Text();

			boolean found = false;
			int occ = 0;

			for (Text val : values) {

				String[] v = val.toString().split(" ");
				// If we don't have the amount of occurences of w1, w2 --> We save the value in our list (ArrayList)
				// and keep looking.
				if (!found){
					if (v.length == 1) {
						// 1. save the amount of occurrences in a "global" variable.
						// 2. Perform the writing of all the values in our list to the context.
						// 3. Clean the data-structure (list).
						found = true;
					}
					else{
						// We save the whole string, containing w1, w2, w3, the amount of their occurrences
						// delimited by a space.
						vals.add(val.toString());
					}
				}
				// We have the amount of occurances of the two words w1, w2 --> Can just send the desired <key, value>
				// to the context.
				else{
					// Just send to context <"w1 w2 w3", "w1 w2 (occ_w1w2)">
				}

			}


/*
			int occu = 0;

			boolean b1=false;
			boolean b2=false;

			for (Text val : values) {

				String[] vs = val.toString().split(" ");

				if(vs.length>1){
					newKey.set(String.format("%s %s %s", vs[0], vs[1], vs[2]));
					b1=true;
				}
				else{
					occu = (int) Long.parseLong(vs[0]);
					newVal.set(String.format("%s %s %d",w1,w2,occu));
					b2 = true;
				}
				if(b1 && b2){
					context.write(newKey, newVal);
					b1 = false;
				}
			}
*/


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
			Job job = Job.getInstance(conf, "Aggregate 2 and 3");
			job.setJarByClass(step4.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setPartitionerClass(myPartitioner.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			String input1="/output2/";
			String input2="/output3/";
			String output="/output4/";
			MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
			MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
		}


}
