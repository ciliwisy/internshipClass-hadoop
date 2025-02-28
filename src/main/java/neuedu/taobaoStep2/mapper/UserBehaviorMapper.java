package neuedu.taobaoStep2.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserBehaviorMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
private final Text outKey = new Text();
private final LongWritable outValue = new LongWritable(1);
@Override
protected void map(LongWritable key, Text value, Mapper<LongWritable,
        Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
    //1011695,4763804,4789432,pv,2017-12-03,13
    //1001145,5123296,4801426,fav,2017-11-30,10
    String[] keys = value.toString().split(",");
    outKey.set(keys[3]);
    context.write(outKey, outValue);
}
}