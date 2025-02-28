package neuedu.chargerDayAnalysis;

import neuedu.chargerDayAnalysis.Mapper.ChargeDayMapper;
import neuedu.chargerDayAnalysis.Reducer.ChargeDayReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChargeDayAnalysis extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://8.146.207.218:9000");
        Job job = Job.getInstance(conf, "chargeDayAnalysis");
        job.setJarByClass(ChargeDayAnalysis.class);

        // 设置输入输出路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/charger_data_input/nvv2t_md.csv"));

        // 设置Mapper相关配置
        job.setMapperClass(ChargeDayMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer相关配置
        job.setReducerClass(ChargeDayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(conf);
        // 设置输出
        Path savePath = new Path("/charger_data_output/");
        if (hdfs.exists(savePath)){
            hdfs.delete(savePath, true);
        }
        TextOutputFormat.setOutputPath(job, savePath);
        System.out.println("开始执行任务");

        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("任务执行成功");
            return 0;
        }
        else {
            System.out.println("任务执行失败");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new ChargeDayAnalysis(), args);
        System.exit(run);
    }
}