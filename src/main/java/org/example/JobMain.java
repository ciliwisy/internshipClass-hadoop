package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.example.wordcount.mapper.WordCountMapper;
import org.example.wordcount.reducer.WordCountReducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class JobMain extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        // 设置HADOOP使用的用户，如果使用root用户需要设置
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    // 创建一个Job任务对象
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://8.146.207.218:9000");
    Job job = Job.getInstance(conf, "wordcount");
    job.setJarByClass(JobMain.class);

    // 配置Job任务对象（8个步骤）
    // 1. 读取文件，指定用哪个Format并设置HDFS路径(目录就可以，自动读取目录下所有文件)
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path("/user/hadoop/wordcount"));

    // 2. 指定Map阶段的处理方式和数据类型
    job.setMapperClass(WordCountMapper.class);
    // 设置map阶段K2的类型
    job.setMapOutputKeyClass(Text.class);
    // 设置map阶段V2的类型
    job.setMapOutputValueClass(LongWritable.class);

    // 3、4、5、6采用默认方式，暂时不做处理

    // 7. 指定Reduce阶段的处理方式和数据类型
    job.setReducerClass(WordCountReducer.class);
    // 设置K3的类型
    job.setOutputKeyClass(Text.class);
    // 设置V3的类型
    job.setOutputValueClass(LongWritable.class);

    // 8. 设置输出类型和输出路径
    job.setOutputFormatClass(TextOutputFormat.class);
    // 结果输出路径
    TextOutputFormat.setOutputPath(job, new Path("/wordcount/out"));

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
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, (Tool) new JobMain(), args);
        System.exit(run);
    }
}
