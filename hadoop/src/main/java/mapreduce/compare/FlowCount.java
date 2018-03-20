package mapreduce.compare;

import mapreduce.Partitioner.ProvincePartitioner;
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
public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable, Text, FlowBean,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split(" ");
            context.write(new FlowBean(Integer.getInteger(words[3]),Integer.getInteger(words[4])),new Text(words[0]));
        }

    }

    static class FlowCountReducer extends Reducer<FlowBean,Text,Text,FlowBean> {

        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text phone = values.iterator().next();
            context.write(phone,key);
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCount.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        FileInputFormat.setInputPaths(job, new Path("/home/user/IdeaProjects/github/BigData/data/input/phone"));
        FileOutputFormat.setOutputPath(job, new Path("/home/user/IdeaProjects/github/BigData/data/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }

}
