package org.example.wordcount.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    protected void map (LongWritable key,Text value,Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws IOException, InterruptedException, IOException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        String[] result = value.toString().split(",");
        for(String val:result){
            text.set(val);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}
