package big.data.hse.hw.hw2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sievmi on 23.09.18
 */

public class SortingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable offset, Text text, Context context)
            throws IOException, InterruptedException {
        String[] splitted = text.toString().trim().split("\t");
        if (splitted.length == 2) {
            String word = splitted[0];
            Double value = Double.parseDouble(splitted[1]);
            context.write(new Text(String.format("%10.2f", value) + "_" + word),
                    new DoubleWritable(value));
        }
    }
}
