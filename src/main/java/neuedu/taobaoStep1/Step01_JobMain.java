package neuedu.taobaoStep1;

import neuedu.taobaoStep1.mapper.Step01_Mapper;
import neuedu.taobaoStep1.reducer.Step01_Reducer;
import neuedu.wordcount.JobMain;
import neuedu.wordcount.WordCountMapper;
import neuedu.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Step01_JobMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        // 启动Job任务，使用ToolRunner
        // ToolRunner：
        // 第一个参数：config类，
        Configuration conf = new Configuration();
        // 第二个参数，Tool接口，需要传实现类对象，也就是JobMain类本身
        // 第三个参数，输入的参数，用当前main方法的入参即可

        // 返回值，0表示任务成功，否则表示任务失败
        int run = ToolRunner.run(conf, new Step01_JobMain(), args);
        // 进程退出
        System.exit(run);

    }

    // 该方法用于指定一个job任务（执行哪个mapper类、哪个reducer类等等）
    @Override
    public int run(String[] strings) throws Exception {
        // 设置HADOOP使用的用户，如果使用root用户需要设置
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 创建一个Job任务对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-svr-01:9000");
        Job job = Job.getInstance(conf, "TaoBao_Step01");
        job.setJarByClass(Step01_JobMain.class);
        // 配置Job任务对象（8个步骤）
        // 1. 读取文件，指定用哪个Format并设置HDFS路径(目录就可以，自动读取目录下所有文件)
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/taobao_data_input"));

        // 2. 指定Map阶段的处理方式和数据类型
        job.setMapperClass(Step01_Mapper.class);
        // 设置map阶段K2的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置map阶段V2的类型
        job.setMapOutputValueClass(Text.class);

        // 3、4、5、6采用默认方式，暂时不做处理

        // 7. 指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(Step01_Reducer.class);
        // 设置K3的类型
        job.setOutputKeyClass(NullWritable.class);
        // 设置V3的类型
        job.setOutputValueClass(Text.class);

        // 8. 设置输出类型和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        // 结果输出路径
        // 如果存在结果目录则先删除目录
        FileSystem hdfs = FileSystem.get(conf);
        Path savePath = new Path("/taobao_data_output/Step01");
        if (hdfs.exists(savePath)){
            hdfs.delete(savePath, true);
        }

        TextOutputFormat.setOutputPath(job, savePath);

        // 等待任务结束，flag代表着任务成果和失败
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