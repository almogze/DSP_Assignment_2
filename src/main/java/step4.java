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
			String[] keyVal = value.toString().split("\t");

			String[] words = keyVal[0].split(" ");
			String w1 = words[0];
			String w2 = words[1];   

			// int occ = Integer.parseInt(keyVal[1]);
			String occ = keyVal[1];
			Text occurs = new Text(occ);
			// occurs.set(String.format("%d",occ));

			Text firstTwo = new Text(String.format("%s %s",w1,w2));
			// firstTwo.set(String.format("%s %s",w1,w2));

			if(words.length>2){
				String w3 = words[2];
				Text newVal = new Text(String.format("%s %s %s %s",w1,w2,w3,occ));
				// t.set(String.format("%s %s %s %d",w1,w2,w3,occ));
				Text lastTwo = new Text(String.format("%s %s",w2,w3));
				// lastTwo.set(String.format("%s %s",w2,w3));
				context.write(firstTwo, newVal);
				context.write(lastTwo, newVal);
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
			// until we find the amount of occurrences of "w1 w2" in the corpus.

			List<String> savedVals = new ArrayList<>();


			String[] words = key.toString().split(" ");
			String w1 = words[0];
			String w2 = words[1];

			Text newKey = new Text();
			Text newVal = new Text();

			boolean found = false;
			int occ2 = 0;			// The number of occurrences of "w1 w2" in the corpus.

			for (Text val : values) {

				String[] v = val.toString().split(" ");
				// If we don't have the amount of occurences of w1, w2 --> We save the value in our list (ArrayList)
				// and keep looking.
				if (!found){
					if (v.length == 1) {
						// 1. save the amount of occurrences in a "global" variable.
						occ2 = Integer.parseInt(v[0]);

						// 2. Perform the writing of all the values in our list to the context.
						for (String threes : savedVals){
							String[] vs = threes.split(" ");

							newKey = new Text(String.format("%s %s %s", vs[0], vs[1], vs[2]));
							// New value: occ3, w1, w2, occ2
							newVal = new Text(String.format("%s %s %s %d", vs[3], w1, w2, occ2));
							context.write(newKey, newVal);

						}

						// 3. Clean the data-structure (list).
						savedVals.clear();

						found = true;
					}
					else{
						// We save the whole string, containing w1, w2, w3, the amount of their occurrences - occ3
						// delimited by a space.
						savedVals.add(val.toString());
					}
				}
				// We have the amount of occurances of the two words w1, w2 --> Can just send the desired <key, value>
				// to the context.
				else{
					// Just send to context <"w1 w2 w3", "occ3 w1 w2 (occ_w1w2)">
					String[] vs = val.toString().split(" ");
					newKey = new Text(String.format("%s %s %s", vs[0], vs[1], vs[2]));
					// New value: occ3, w1, w2, occ2
					newVal = new Text(String.format("%s %s %s %d", vs[3], w1, w2, occ2));
					context.write(newKey, newVal);
				}

			}
		}
	}
	
	  private static class myPartitioner extends Partitioner<Text, Text>{
			@Override
			public int getPartition(Text key, Text value, int numPartitions){
				
				return Math.abs(key.hashCode()) % numPartitions;
			}
	    	
	    }
	    
	    public static void main(String[] args) throws Exception {
			System.out.println("Entered main of step1");


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
			MultipleInputs.addInputPath(job, new Path("s3://assignment1razalmog121212/output/2gram/"), TextInputFormat.class);
			MultipleInputs.addInputPath(job, new Path("s3://assignment1razalmog121212/output/3gram/"), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
		}


}
