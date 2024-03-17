package com.MRItemCF;

import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;
import java.util.Map;

public class StartRun {
    public static void main(String[] args) {
        Configuration config = new Configuration();
        Map<String, String> paths = new HashMap<String, String>();
        //windows路径
        paths.put("Step1Input", "E:\\MavenProject\\Recommend\\data\\test2.csv"); // 原始数据集
        paths.put("Step1Output", "E:\\MavenProject\\Recommend\\data\\output1");
        paths.put("Step2Input", paths.get("Step1Output"));
        paths.put("Step2Output", "E:\\MavenProject\\Recommend\\data\\output2");
        paths.put("Step3Input", paths.get("Step2Output"));
        paths.put("Step3Output", "E:\\MavenProject\\Recommend\\data\\output3");
        paths.put("Step4Input1", paths.get("Step2Output"));
        paths.put("Step4Input2", paths.get("Step3Output"));
        paths.put("Step4Output", "E:\\MavenProject\\Recommend\\data\\output4");
        paths.put("Step5Input", paths.get("Step4Output"));
        paths.put("Step5Output", "E:\\MavenProject\\Recommend\\data\\output5");
        paths.put("Step6Input", paths.get("Step5Output"));
        paths.put("Step6Output", "E:\\MavenProject\\Recommend\\data\\output6");

//        //linux路径
//        paths.put("Step1Input", "/user/itemcf/input/ali_t.csv"); // 数据集 datasets
//        paths.put("Step1Output", "/user/itemcf/output/step1");
//        paths.put("Step2Input", paths.get("Step1Output"));
//        paths.put("Step2Output", "/user/itemcf/output/step2");
//        paths.put("Step3Input", paths.get("Step2Output"));
//        paths.put("Step3Output", "/user/itemcf/output/step3");
//        paths.put("Step4Input1", paths.get("Step2Output"));
//        paths.put("Step4Input2", paths.get("Step3Output"));
//        paths.put("Step4Output", "/user/itemcf/output/step4");
//        paths.put("Step5Input", paths.get("Step4Output"));
//        paths.put("Step5Output", "/user/itemcf/output/step5");
//        paths.put("Step6Input", paths.get("Step5Output"));
//        paths.put("Step6Output", "/user/itemcf/output/step6");
//
//
        System.out.println("========================start step1=========================");
        Step1.run(config, paths);	 // 格式化 去重
        System.out.println("========================start step2=========================");
		Step2.run(config, paths);	// 计算得分矩阵
        System.out.println("========================start step3=========================");
		Step3.run(config, paths);	// 计算同现矩阵
        System.out.println("========================start step4=========================");
		Step4.run(config, paths);	// 同现矩阵和得分矩阵相乘
        System.out.println("========================start step5=========================");
		Step5.run(config, paths);	// 把相乘之后的矩阵相加获得结果矩阵
        System.out.println("========================start step6=========================");
        Step6.run(config, paths);	// 排序推荐
    }

	public static Map<String, Integer> R = new HashMap<String, Integer>();
	static {
		R.put("listen", 1);//听过
		R.put("like", 3);//喜欢
		R.put("comment", 4);//评论
		R.put("share", 4);//转发
	}
}
