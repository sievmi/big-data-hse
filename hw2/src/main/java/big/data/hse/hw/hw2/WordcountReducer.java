package big.data.hse.hw.hw2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by sievmi on 23.09.18
 */

public class WordcountReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        int count = 0;
        for (IntWritable current : values) {
            sum += (long)current.get();
            count += 1;
        }
        context.write(key, new DoubleWritable(sum * 1.0 / count));
    }

}