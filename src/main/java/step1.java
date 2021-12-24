import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;

public class step1 {
    /**
     * Input to the mapper:
     * Key: Line number (not important).
     * Value: (1-gram - the actual word,
     *        year of this aggregation,
     *        occurrences in this year,
     *        pages - The number of pages this 1-gram appeared on in this year,
     *        books - The number of books this 1-gram appeared in during this year)
     *
     * Output of Mapper:
     *        Key: The word.
     *        Value: The amount of times it appeares in the year of this record
     *               OR an asterisk (used to count total amount of words in the corpus later).
     *
     * Input of Reducer:
     *        Output of mapper.
     *
     * Output of Reducer:
     *        Key: a single word.
     *        Value: The total amount of times it appears in the corpus.
     *
     *        Notice that this is practically word-count.
     *
     *	Example input:
     *
     * 	Example output:
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            String w1 = vals[0];

            System.out.println(w1);     // Check if this is also a tuple or just a word (we think it's a word)

            Text text = new Text(w1);
            // text.set(w1);
            Text occurences = new Text(vals[2]);
            // occurences.set(vals[2]);
            Text text2 = new Text("*");
            // text2.set("*");
            context.write(text,occurences);
            context.write(text2,occurences);
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
    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumOccurences = 0;

            for (Text occ : values) {
                sumOccurences += Long.parseLong(occ.toString());
            }

            Text newVal = new Text(String.format("%d",sumOccurences));
            // newVal.set(String.format("%d",sumOccurences));

            // We send the same key, with the total amount of it's appearences in the corpus.
            context.write(key, newVal);
        }
    }


    private static class PartitionerClass extends Partitioner<Text,Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions){
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
        System.out.println("Entered main of step1");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "1gram");
        job.setJarByClass(step1.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(step1.PartitionerClass.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        String output="/output1/";
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}





