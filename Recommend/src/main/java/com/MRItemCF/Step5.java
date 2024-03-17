/**
 * 把相乘之后的矩阵相加获得结果矩阵
 */
package com.MRItemCF;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step5 {
    private final static Text K = new Text();
    private final static Text V = new Text();

    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step5");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step5_Mapper.class);
            job.setReducerClass(Step5_Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat
                    .addInputPath(job, new Path(paths.get("Step5Input")));
            Path outpath = new Path(paths.get("Step5Output"));
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

    static class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        //原封不动输出
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text(tokens[1] + "," + tokens[2]);
            context.write(k, v);
        }
    }

    //累加 KEY=用户id	VALUE=音乐id:相乘结果
    static class Step5_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap<String, Double>();// 结果

            for (Text line : values) {// m1009,8.0
                String[] tokens = line.toString().split(",");
                String musicID = tokens[0];	// 音乐id
                Double score = Double.parseDouble(tokens[1]); // 相乘结果
                if (map.containsKey(musicID)) { // 累加更新
                    map.put(musicID, map.get(musicID) + score);// 矩阵乘法求和计算
                } else {
                    map.put(musicID, score); // 初始累加
                }
            }

            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String musicID = iter.next();
                double score = map.get(musicID);
                Text v = new Text(musicID + "," + score);
                context.write(key, v);
            }
        }
        // 样本:  u1  m1001,5.0
    }
}

