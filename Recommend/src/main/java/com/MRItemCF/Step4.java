/**
 * 把同现矩阵和得分矩阵相乘
 */
package com.MRItemCF;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step4 {

    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step4");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step4_Mapper.class);
            job.setReducerClass(Step4_Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path[] { new Path(paths.get("Step4Input1")), new Path(paths.get("Step4Input2")) });
            Path outpath = new Path(paths.get("Step4Output"));
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

    static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;// A同现矩阵 or B得分矩阵

        // 每个maptask初始化时调用一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据是output2还是output3
            System.out.println(flag + "**********************");
        }

        /**
         * map输出数据格式为		KEY:物品id	VALUE: A:同现矩阵 B:得分矩阵
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());

            if (flag.equals("output3")) {// 同现矩阵 m1001:m1009 3
                String[] v1 = tokens[0].split(":");
                String music1 = v1[0];
                String music2 = v1[1];
                String num = tokens[1];

                Text k = new Text(music1);// m1001
                Text v = new Text("A:" + music2 + "," + num);// A:m1009,3
                System.out.println(k.toString()+" "+v.toString());

                context.write(k, v);

            } else if (flag.equals("output2")) {// 用户对音乐的喜爱得分矩阵
                // u1 m1001:2 m1004:4 变成这种格式是因为在上面进行了Pattern.compile
                String userID = tokens[0];//u1
                for (int i = 1; i < tokens.length; i++) {
                    String[] vector = tokens[i].split(":");
                    String itemID = vector[0];// 音乐id  m1001
                    String pref = vector[1];// 喜爱分数  2

                    Text k = new Text(itemID); // 以音乐为key  m1001
                    Text v = new Text("B:" + userID + "," + pref); // B:u1,2
                    System.out.println(k.toString()+" "+v.toString());

                    context.write(k, v);
                }
            }
        }
    }

    //把相同音乐id的A和B矩阵放在一起
    static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // A同现矩阵 or B得分矩阵

            // 某一音乐，针对它和其他所有音乐的同现次数，都在mapA集合中
            Map<String, Integer> mapA = new HashMap<String, Integer>();
            //其他音乐ID为map的key，同现数字为value

            Map<String, Integer> mapB = new HashMap<String, Integer>();
            //该音乐（key中的musicID），所有用户的推荐权重分数

            for (Text line : values) {
                String val = line.toString();
                if (val.startsWith("A:")) {// 表示音乐同现数字
                    //key:m1001   value: A:m1003,3
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    //从第二个位置之后的子串，就是相当于删掉了"A:"
                    try {
                        mapA.put(kv[0], Integer.parseInt(kv[1])); // 同现矩阵初始化 音乐id:同时出现次数
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else if (val.startsWith("B:")) { //得分
                    // key:m1001  value B:u1,2
                    String[] kv = Pattern.compile("[\t,]").split(
                            val.substring(2));
                    try {
                        mapB.put(kv[0], Integer.parseInt(kv[1]));	//用户id:得分
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }


            double result = 0;
            Iterator<String> itera = mapA.keySet().iterator();
            while (itera.hasNext()) {
                String tmpmusic = itera.next();// 音乐ID
                int num = mapA.get(tmpmusic).intValue(); // 同时出现次数

                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String tmpuser = iterb.next();// userID
                    int pref = mapB.get(tmpuser).intValue(); // 权重
                    result = num * pref;// 矩阵乘法相乘计算

                    Text k = new Text(tmpuser); //用户id
                    Text v = new Text(tmpmusic + "," + result);
                    context.write(k, v);
                }
            }
            //  结果样本:  u1  m1001,8.0
        }
    }
}

