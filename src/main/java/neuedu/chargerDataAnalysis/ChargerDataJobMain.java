package neuedu.chargerDataAnalysis;

import neuedu.chargerDataAnalysis.mapper.ChargerMapper;
import neuedu.chargerDataAnalysis.reducer.ChargerReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class ChargerDataJobMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int run = ToolRunner.run(conf,new ChargerDataJobMain(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        //设置执行用户
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://8.146.207.218:9000");
        Job job = Job.getInstance(conf, "ChargerDataAnalysis");

        //配置运行对象
        job.setJarByClass(ChargerDataJobMain.class);
        job.setMapperClass(ChargerMapper.class);
        job.setReducerClass(ChargerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(conf);

        TextInputFormat.addInputPath(job, new Path("/charger_data_input/nvv2t_md.csv"));
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
}
