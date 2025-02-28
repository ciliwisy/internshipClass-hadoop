package neuedu.taobaoStep1.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Step01_Mapper extends Mapper<LongWritable, Text, Text, Text>
{
    // 定义输出数据类型
    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        long timestamp = Long.parseLong(values[4]);
        // 创建 Date 对象，注意：Date 构造函数接受毫秒数，因此需要乘以 1000
        Date date = new Date(timestamp * 1000L);
        // 格式化日期，获取yyyy-MM-dd和HH，因为后续会用逗号拼接字符串，所以这里直接
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd,HH");

        String newVal = values[0] + "," +
                values[1] + "," +
                values[2] + "," +
                values[3] + "," +
                sdf.format(date);
        outKey.set(values[0]);
        outValue.set(newVal);
        context.write(outKey, outValue);
    }
}