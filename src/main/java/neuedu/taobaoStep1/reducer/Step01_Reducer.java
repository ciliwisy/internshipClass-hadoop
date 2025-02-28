package neuedu.taobaoStep1.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class Step01_Reducer extends Reducer<Text, Text, NullWritable, Text
        > {
    private final NullWritable outKey = NullWritable.get();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        // 此时的values中的每个值就是巨有相同商品ID的处理后数据，正好当作按着ID排序了
        String str = "";
        for (Text item : values) {
            str = item.toString();
            outValue.set(str);
            context.write(outKey, outValue);
        }
    }
}