package big.data.hse.hw.hw2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sievmi on 24.09.18
 */

public class SortingReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        String[] splitted = key.toString().split("_");
        String value = splitted[0].trim();
        String word = splitted[1];

        context.write(new Text(word), new Text(word + "\t" + value));
    }

}