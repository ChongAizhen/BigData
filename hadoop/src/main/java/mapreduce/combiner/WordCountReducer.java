package mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by chongaizhen on 2018/03/12.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

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
