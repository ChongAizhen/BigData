package mapreduce.FileFormat.MultipleOutputs;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by chongaizhen on 2018/04/09.
 */

/*
旧API中有org.apache.hadoop.mapred.lib.MultipleOutputFormat和org.apache.hadoop.mapred.lib.MultipleOutputs两个重要的类，
但是由于旧版本的MultipleOutputFormat是基于行的划分而MultipleOutputs是基于列的划分。所以在新的API中就剩下了MultipleOutputs(mapreduce包中)类，
这个类合并了旧API中的MultipleOutputFormat和MultipleOutputs的功能，同时新版的类库中已经不存在MultipleOutputFormat类了
 */
public class WordCount {

    static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        private MultipleOutputs mos;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
        }

        //输入的key为当前行的起始偏移量，输入的value为这一行的内容
        //每读取一行调用一次map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (int i=0;i<words.length;i++){
                mos.write(new Text(words[i]),new IntWritable(1),generateFileName(new Text(words[i])));
            }
        }

        private String generateFileName(Text value) {
            String word = value.toString();
            return word+"/";
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }

    static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        //每传进来一个kv组，调用一次reduce方法
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
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setNumReduceTasks(0);

        //设置我们的业务逻辑Mapper类的输出key和value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置我们的业务逻辑Reducer类的输出key和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(job,new Path("E:\\IdeaProjects\\git\\BigData\\data\\input\\test1"));
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job,new Path("E:\\IdeaProjects\\git\\BigData\\data\\output"));


        //向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}