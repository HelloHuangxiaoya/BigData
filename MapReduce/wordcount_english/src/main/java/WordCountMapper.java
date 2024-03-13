import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text text = new Text();
    IntWritable one = new IntWritable(1);
    Pattern wordPattern = Pattern.compile("\\b[a-zA-Z']+\\b");//"\\b[\\w']+\\b"
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("Key:"+key+" Value:"+value);
        Matcher matcher=wordPattern.matcher(value.toString());
        while(matcher.find()){
            text.set(matcher.group().toLowerCase());
            context.write(text,one);
            //System.out.println("<"+text+","+one+">");
        }
    }
}