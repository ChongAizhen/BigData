package mapreduce.Partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Chong AiZhen on 18-3-20,下午5:24.
 */
public class WordCount {

    static class WordCountMapper extends Mapper<LongWritable, Text, Text,IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split(" ");
            for (int i=0;i<words.length;i++){
                context.write(new Text(words[i]),new IntWritable(1));
            }

        }

    }

    static class WordCountReducer extends Reducer<Text,IntWritable,Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (IntWritable value:values){
                count+=value.get();
            }
            context.write(key,new IntWritable(count));
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/home/user/IdeaProjects/github/BigData/data/input/test1"));
        FileOutputFormat.setOutputPath(job, new Path("/home/user/IdeaProjects/github/BigData/data/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }

}
