package org.example.wordcount.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //1:遍历集合，将集合中的数字相加，得到 V3
        long cnt = 0L;
        LongWritable longWritable = new LongWritable();
        for (LongWritable value : values){
            //2:将K3和V3写入上下文中
            cnt += value.get();
        }
        longWritable.set(cnt);
        context.write(key, longWritable);
    }
}
