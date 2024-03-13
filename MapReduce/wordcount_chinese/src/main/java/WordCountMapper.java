import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text text = new Text();
    IntWritable one = new IntWritable(1);
    List<String> vocabulary = Arrays.asList("宝玉","黛玉","宝钗","凤姐","贾母","袭人","贾政","贾琏","湘云","平儿");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.print("map函数---");
        System.out.println("Key:"+key+" Value:"+value);
        String line = value.toString();
        for (int i = 0; i < line.length()-1; i++) {
            String word = line.substring(i, i+2);
            if (vocabulary.contains(word)) {
                text.set(word);
                context.write(text,one);
                System.out.println("<"+text+","+one+">");
            }
        }
    }
}