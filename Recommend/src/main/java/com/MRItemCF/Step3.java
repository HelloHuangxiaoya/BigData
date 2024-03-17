/**
 * 对音乐组合列表进行计数，建立物品的同现矩阵		wordcount
 m1001:m1001	3
 m1001:m1005	1
 m1001:m1006	1
 m1001:m1009	1
 */
package com.MRItemCF;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step3 {
    private final static Text K = new Text();
    private final static IntWritable V = new IntWritable(1);

    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step3");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step3_Mapper.class);
            job.setReducerClass(Step3_Reducer.class);
            job.setCombinerClass(Step3_Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat
                    .addInputPath(job, new Path(paths.get("Step3Input")));
            Path outpath = new Path(paths.get("Step3Output"));
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // 第二个MR执行的结果--作为本次MR的输入  样本: u1 m1001:2,m1002:3
    static class Step3_Mapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //这里的key应该是偏移量，value才是一行的内容
            String[] tokens = value.toString().split("\t");
            System.out.println("-----------------------------");
            System.out.println("用户"+tokens[0]+"的矩阵");
            //tokens[0]是用户id
            String[] musics = tokens[1].split(","); //m1001:2  m1002:3
            for (int i = 0; i < musics.length; i++) {
                String musicA = musics[i].split(":")[0];
                for (int j = 0; j < musics.length; j++) {
                    String musicB = musics[j].split(":")[0];
                    K.set(musicA + ":" + musicB);
                    System.out.println(musicA + ":" + musicB);
                    context.write(K, V); // 实际上是进行一次wordcount <音乐id:音乐id , 1>  <m1:m2 , 1>
                }
            }
        }
    }

    //reducer会把每个用户的矩阵加起来，相当于词频统计
    //用户1的矩阵中有m1001:m1002 1
    //用户2的矩阵中有m1001:m1002 1   ----> m1001:m1002 2
    //我的想法：可以通过文件把这个同现矩阵打印出成真正矩阵的形式
    static class Step3_Reducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> i, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : i) {
                sum = sum + v.get();
            }
            V.set(sum);
            context.write(key, V);
        }
    }
}