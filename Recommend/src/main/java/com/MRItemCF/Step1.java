/**
 * 第一步：去掉表头和去重操作
 */
package com.MRItemCF;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1 {
    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step1");
            job.setJarByClass(Step1.class);

            job.setMapperClass(Step1_Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setReducerClass(Step1_Reducer.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
            Path outpath = new Path(paths.get("Step1Output"));
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
    //NullWritable当<key,value>中的key或value为空时使用
    static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() != 0) { //第一行的偏移量为0，删除第一行
                context.write(value, NullWritable.get());
            }
        }
    }

    static class Step1_Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> i, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());//相当于原封不动把map的输出全部写到输出里
        }
    }
}
