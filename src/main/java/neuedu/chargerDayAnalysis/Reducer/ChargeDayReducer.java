package neuedu.chargerDayAnalysis.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ChargeDayReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalChargeTime = 0;
        
        // 计算该时段的总充电时长和充电次数
        for (Text val : values) {
            totalChargeTime += Double.parseDouble(val.toString());
        }

        
        // 输出格式：总充电时长,充电次数,平均充电时长
        result.set(String.format("%.2f", totalChargeTime));
        context.write(key, result);
    }
} 