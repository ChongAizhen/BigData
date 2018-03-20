package mapreduce.FileFormat.CombineFileInputFormat;

import mapreduce.FileFormat.WordCountMapper;
import mapreduce.FileFormat.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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

        //如果不设置InputFormat，它默认用的是TextInputformat.class
        //计算切片大小的逻辑：Math.max(minSize, Math.min(maxSize, blockSize))

        //小文件的处理方法（小文件数量过多会导致map数目过多）：
        //采用CombineFileInputFormat，小文件会进行合并切片的大小会尽量满足最小值，但绝对不会超过最大值
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(job,new Path("file:/home/user/IdeaProjects/github/BigData/data/input"));
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job,new Path("file:/home/user/IdeaProjects/github/BigData/data/output"));


        //向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
