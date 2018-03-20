package mapreduce.Partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by Chong AiZhen on 18-3-20,下午4:57.
 */

/*
模拟根据手机号开头得到不同的省份
 */
public class ProvincePartitioner extends Partitioner<Text,IntWritable>{

    static HashMap<String, Integer> provinceMap = new HashMap<String, Integer>();

    static {
        provinceMap.put("hello", 0);
    }

    public int getPartition(Text key, IntWritable value, int i) {

        //将所有的hello放到一个分区，其余的单词放到另一个分区，需要保证reduce的个数和分区数一致，这里应该为2
        //测试当reduce个数为1时没有进行分区，当reduce个数为3时只产生了两个分区
        Integer code = provinceMap.get(key.toString());
        return code==null?1:code;
    }
}
