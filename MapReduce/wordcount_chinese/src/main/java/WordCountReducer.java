import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    IntWritable results = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        System.out.print("reduce函数---");
        System.out.print("Key:"+key+" Values:");
        int counts=0;
        for (IntWritable value:values){
            System.out.print(value+" ");
            counts+=value.get();
        }
        System.out.println();
        results.set(counts);
        System.out.println("<"+key+","+results.get()+">");
        context.write(key,results);
    }
}

