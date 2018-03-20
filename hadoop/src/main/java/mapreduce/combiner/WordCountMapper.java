package mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by chongaizhen on 2018/03/12.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

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
