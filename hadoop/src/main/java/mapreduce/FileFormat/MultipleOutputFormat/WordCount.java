package mapreduce.FileFormat.MultipleOutputFormat;

import mapreduce.FileFormat.Compression.Compression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by chongaizhen on 2018/04/09.
 */
public class WordCount {

    static class WordCountMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,NullWritable,Text> {

        //输入的key为当前行的起始偏移量，输入的value为这一行的内容
        //每读取一行调用一次map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }

    public static class PartitionFormat
            extends MultipleTextOutputFormat {
        protected String generateFileNameForKeyValue(
                NullWritable key,
                Text value,
                String name) {
            String[] split = value.toString().split(",", -1);
            String country = split[4].substring(1, 3);
            return country + "/" + name;
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        JobConf job = new JobConf(conf, WordCount.class);
        String[] remainingArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Error!");
            System.exit(1);
        }
        Path in = new Path(remainingArgs[0]);
        Path out = new Path(remainingArgs[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("Output");
        job.setMapperClass(WordCountMapper.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(PartitionFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        JobClient.runJob(job);
    }
}
