package neuedu.chargerDataAnalysis.mapper;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

import java.io.IOException;
public class ChargerMapper extends Mapper<LongWritable, Text, Text, Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");


        // 跳过第一行（通常是 CSV 头部）
        String line = value.toString();
        if (line.startsWith("id,name") || key.get() == 0) {
            return;  // 直接跳过，不执行后续逻辑
        }


        String stationId = values[11];
        double chargeTime = Double.parseDouble(values[7]);

        context.write(new Text(stationId), new Text(String.valueOf(chargeTime)));

    }
}
