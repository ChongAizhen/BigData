package mapreduce.compare;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Chong AiZhen on 18-3-20,下午5:54.
 */
public class FlowBean implements WritableComparable<FlowBean>{

    private int upFlow;
    private int downFlow;
    private int totalFlow;

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(int totalFlow) {
        this.totalFlow = totalFlow;
    }

    //序列化需要显示定义一个空参构造方法
    public FlowBean(){}

    public FlowBean(int upFlow,int downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow+downFlow;
    }

    public int compareTo(FlowBean o) {
        return totalFlow>o.getTotalFlow()?-1:1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(totalFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        upFlow=dataInput.readInt();
        downFlow=dataInput.readInt();
        totalFlow=dataInput.readInt();
    }
}
