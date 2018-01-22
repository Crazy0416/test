import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EquiJoin {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text D_key = new Text();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      int cnt = 0;
      StringTokenizer itr = new StringTokenizer(value.toString(), " []");
      String wordData = "";
      while (itr.hasMoreTokens()) {
        String s_word = itr.nextToken();
        wordData += (s_word + " ");
        if(cnt == 1){
                String cleanKey = s_word.replaceAll("[^0-9]","");
                D_key.set(cleanKey);
                System.out.println("cleanKey : " + cleanKey);
        }
        cnt++;
      }
      word.set(wordData);
      System.out.println(wordData + "\n\n");
      context.write(D_key, word);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String dumpString = "";
      for (Text val : values) {
        System.out.println("val : " + val.toString());
        dumpString += val.toString();
      }
      System.out.println("\n\n");
      dumpString += "[" + dumpString + "]";
      result.set(dumpString);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "equi join");
    job.setJarByClass(EquiJoin.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
