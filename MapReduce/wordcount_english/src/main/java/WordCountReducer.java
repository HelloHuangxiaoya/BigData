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
        //System.out.println("Reduce stage Key:"+key+" Values:"+values.toString());
        int counts=0;
        for (IntWritable value:values){
            counts+=value.get();
        }
        results.set(counts);
        //System.out.println("Key:"+key+" ResultValue:"+results.get());
        context.write(key,results);
    }
}
