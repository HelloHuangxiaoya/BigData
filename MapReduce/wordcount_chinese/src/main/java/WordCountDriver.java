import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount_chineses");
        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\MavenProject\\wordcount_chinese\\input\\红楼梦节选.txt"));
        Path path = new Path("E:\\MavenProject\\wordcount_chinese\\output");
        FileSystem fileSystem = FileSystem.get(path.toUri(),conf);
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileSystem fs =FileSystem.get(conf);
//        if(fs.exists(new Path(args[1]))){
//            fs.delete(new Path(args[1]),true);
//        }
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean flag = job.waitForCompletion(true);
        if(flag){
            System.out.println("wordcount_chinese success!");
        }
        if (!flag) {
            System.out.println("wordcount_chinese failed!");
        }
    }
}
