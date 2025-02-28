package neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;

public class DataPreparation {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://8.146.207.218:9000");

        FileSystem fs = FileSystem.get(conf);

        Path path = new Path("C:\\Users\\lenovo\\IdeaProjects\\untitled\\src\\main\\resources\\UserBehavior_part_data.csv");
        Path dst = new Path("/taobao_data_input/UserBehavior_part_data.csv");

        fs.copyFromLocalFile(path,dst);

        FileStatus[] statuses = fs.listStatus(new Path("/wordcount"));
        Arrays.stream(statuses).forEach(System.out::println);


    }
}
