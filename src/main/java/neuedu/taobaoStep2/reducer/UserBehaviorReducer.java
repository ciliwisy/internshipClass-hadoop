package neuedu.taobaoStep2.reducer;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserBehaviorReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer
            <Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long cnt = 0;
        for (LongWritable value : values) {
            cnt++;
        }
        outValue.set(cnt);
        context.write(key, outValue);
    }
}