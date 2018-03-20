package mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Chong AiZhen on 18-3-20,下午2:46.
 */

/*
1.combiner是在每一个maptask所在的节点运行，Reducer是接收全局所有Mapper的输出结果；
2.combiner的意义就是对每一个maptask的输出进行局部汇总，以减小网络传输量
3.combiner能够应用的前提是不能影响最终的业务逻辑，而且，combiner的输出kv应该跟reducer的输入kv类型要对应起来
（影响逻辑的例如求平均值：combine阶段（1+3）/2=2，（2+3+4）/3=3，reduce阶段（2+3）/2=2.5，而实际结果为（1+3+2+3+4）/5=2.6）

以wordcount为例，假如maptask中的结果为<(hello,1),(hello,1),(hadoop,1)>，那么传递到reduce时的结果就会变成<(hello,2),(hadoop,1)>

什么时候运行Combiner？
1、当job设置了Combiner，并且spill的个数到min.num.spill.for.combine(默认是3)的时候，那么combiner就会Merge之前执行；
2、但是有的情况下，Merge开始执行，但spill文件的个数没有达到需求，这个时候Combiner可能会在Merge之后执行；
3、Combiner也有可能不运行，Combiner会考虑当时集群的一个负载情况。如果集群负载量很大，会尽量提早执行完map，空出资源，所以，就不会去执行。
 */
public class WordCountCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for (IntWritable value:values){
            count+=value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
