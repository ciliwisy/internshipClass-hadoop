package neuedu.chargerDayAnalysis.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class ChargeDayMapper extends Mapper<LongWritable, Text, Text, Text>{
    //定义变量输出格式
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 跳过表头
        if (value.toString().startsWith("sessionId") || key.get() == 0) {
            System.out.println("跳过表头行");
            return;
        }

        String[] fields = value.toString().split(",");
        System.out.println("正在处理行: " + value.toString());
        
        try {
            // 检查数据完整性
            if (fields.length < 22) {
                System.out.println("数据字段不完整，长度：" + fields.length);
                return;
            }

            // 获取星期几的信息（Mon到Sun的值）
            int mon = Integer.parseInt(fields[15]);
            int tue = Integer.parseInt(fields[16]);
            int wed = Integer.parseInt(fields[17]);
            int thu = Integer.parseInt(fields[18]);
            int fri = Integer.parseInt(fields[19]);
            int sat = Integer.parseInt(fields[20]);
            int sun = Integer.parseInt(fields[21]);
            
            // 获取充电时间信息
            String startTime = fields[5];
            String endTime = fields[6];
            
            System.out.println("解析的时间 - 开始: " + startTime + ", 结束: " + endTime);
            
            // 提取开始和结束时间的小时信息
            int startHour = Integer.parseInt(startTime);
            int endHour = Integer.parseInt(endTime);
            
            // 确定是星期几
            String weekday = "";
            if (mon == 1) weekday = "Mon";
            else if (tue == 1) weekday = "Tue";
            else if (wed == 1) weekday = "Wed";
            else if (thu == 1) weekday = "Thu";
            else if (fri == 1) weekday = "Fri";
            else if (sat == 1) weekday = "Sat";
            else if (sun == 1) weekday = "Sun";
            
            System.out.println("识别到的星期: " + weekday);
            
            if (!weekday.isEmpty()) {
                System.out.println("处理时间段: " + startHour + " 到 " + endHour);
                for (int hour = startHour; hour <= endHour; hour++) {
                    outKey.set(weekday + "_" + hour);
                    outValue.set("1");
                    
                    System.out.println("输出键值对: " + outKey + " -> " + outValue);
                    context.write(outKey, outValue);
                }
            } else {
                System.out.println("警告：未能识别星期几");
            }
        } catch (Exception e) {
            System.out.println("处理数据时发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
