import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordcount_partitioner extends Configured implements Tool{

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class FirstLetterPartitioner extends Partitioner<Text, IntWritable> {

	  @Override
	  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	    char firstLetter = Character.toLowerCase(key.toString().charAt(0));

	    if (numReduceTasks == 0) {
	      return 0;
	    }

	    if (firstLetter < 'i') {
	      return 0;
	    } else if (firstLetter < 'q') {
	      return Math.min(1, numReduceTasks - 1);
	    } else {
	        return Math.min(2, numReduceTasks - 1);
	      }
	    }
	  }
  
  public static class WordCountCombiner
  extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
 int sum = 0;
 for (IntWritable val : values) {
   sum += val.get();
 }
 result.set(sum);
 context.write(key, result);
}
}
  
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(wordcount_partitioner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setPartitionerClass(FirstLetterPartitioner.class);
    job.setNumReduceTasks(3);
    job.setCombinerClass(WordCountCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
	  int res = ToolRunner.run(new Configuration(), new wordcount_partitioner(), args);
	    System.exit(res);
  }
}