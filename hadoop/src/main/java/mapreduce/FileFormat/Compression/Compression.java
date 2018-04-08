package mapreduce.FileFormat.Compression;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Compression {

    static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        //输入的key为当前行的起始偏移量，输入的value为这一行的内容
        //每读取一行调用一次map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (int i=0;i<words.length;i++){
                context.write(new Text(words[i]),new IntWritable(1));
            }
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

        //验证结果是mapred.compress.map.out为true以及设置了FileOutputFormat，map和reduce端的输出都会进行压缩
        configuration.setBoolean("mapred.compress.map.out", true);

//        configuration.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
//        configuration.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);


        Job job = Job.getInstance(configuration);

        job.setJarByClass(Compression.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置我们的业务逻辑Mapper类的输出key和value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置我们的业务逻辑Reducer类的输出key和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果不设置InputFormat，它默认用的是TextInputformat.class
        //计算切片大小的逻辑：Math.max(minSize, Math.min(maxSize, blockSize))

        //设置输出文件的压缩
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        //指定要处理的数据所在的位置
        //Hadoop自带的InputFormat类内置支持压缩文件的读取
        FileInputFormat.setInputPaths(job,new Path("file:/home/user/IdeaProjects/github/BigData/data/input/compress.gz"));
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job,new Path("file:/home/user/IdeaProjects/github/BigData/data/output"));


        //向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}