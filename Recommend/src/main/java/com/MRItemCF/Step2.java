/**
 * 计算每个用户对音乐的得分
 * u1 m1001:2,m1004:4,m1009:2
 */
package com.MRItemCF;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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


public class Step2 {
    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step2");
            job.setJarByClass(StartRun.class);

            job.setMapperClass(Step2_Mapper.class);
            job.setReducerClass(Step2_Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
            Path outpath = new Path(paths.get("Step2Output"));
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

    static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        //输入:userid musicid action
        //输出：(userid,musicid:评分)
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String user = tokens[0];	// 用户id	userid
            String music = tokens[1];	// 音乐id	musicid
            String action = tokens[2]; // 用户行为	user action
            Text k = new Text(user);  //用户id作为key
            Integer rv = StartRun.R.get(action);
            // if(rv!=null){
            Text v = new Text(music + ":" + rv.intValue());
            context.write(k, v); //u1 m1001:1
            System.out.println(k+" "+v);
        }
    }
    //在reduce阶段，相同key的，value会构成一个列表
    static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //u1  m1001:1
            //u1  m1001:2   ----> u1  m1001:3 m1002:1
            //u1  m1002:1
            Map<String, Integer> r = new HashMap<String, Integer>();
            for (Text value : values) {
                String[] vs = value.toString().split(":");//value: m1001:3
                String music = vs[0];//m1001
                Integer action = Integer.parseInt(vs[1]);//3
                action = ((Integer) (r.get(music) == null ? 0 : r.get(music))).intValue() + action; // 如果一个物品操作多次,对其权重进行累加
                r.put(music, action);
            }
            StringBuffer sb = new StringBuffer();
            //sb是要新写入的一行
            for (Entry<String, Integer> entry : r.entrySet()) {
                sb.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");
            }
            context.write(key, new Text(sb.toString()));
//            context.write(key, new Text(sb.toString().substring(0,sb.toString().length()-1)));
        }
    }
}

