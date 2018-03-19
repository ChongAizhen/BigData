package mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by chongaizhen on 2018/03/12.
 */
public class WordCountRunner {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountRunner.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置我们的业务逻辑Mapper类的输出key和value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置我们的业务逻辑Reducer类的输出key和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(job,new Path("/home/user/IdeaProjects/github/BigData/hadoop/src/main/java/mapreduce/wordcount/input/test"));
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job,new Path("/home/user/IdeaProjects/github/BigData/hadoop/src/main/java/mapreduce/wordcount/output"));

        //向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
