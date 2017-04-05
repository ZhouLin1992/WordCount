import java.io.File;
//import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
//import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount4 {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
          	/*Scanner scan;
          	
          	Path[] localPaths = context.getLocalCacheFiles();
          	scan = new Scanner (new FileReader(myfile));*/
          	String scan = new Scanner (new File("myfile")).useDelimiter("\\Z").next();
          	//System.out.println(scan);
          	
          	StringTokenizer str1 = new StringTokenizer(scan);
          	
          	while(str1.hasMoreTokens()){
            	  String a = str1.nextToken(); //指向 pattern 文件
         	       //System.out.println(str1.nextToken());
         	      StringTokenizer str2 = new StringTokenizer(value.toString());//指向 input 文件
                while (str2.hasMoreTokens()) {
                    String b = str2.nextToken();
                    if (a.equals(b)){
                        word.set(b);
                        context.write(word, one);
                        //   System.out.println(b);
                    }
                }
          	}
      	}
    }
  
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context
                          ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(3);
        }
        Job job = new Job(conf, "word count");
        DistributedCache.createSymlink(job.getConfiguration());//创建符号链接
        DistributedCache.addCacheFile(new URI(otherArgs[2]+"#myfile"), job.getConfiguration());//加入分布式缓存,myfile是符号
        //DistributedCache.addCacheFile(new URI("/user/hadoopword-patterns.txt#myfile")),job.getConfiguration());
        job.setJarByClass(WordCount4.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}