package big.data.hse.hw.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sievmi on 09.10.18
 */
public class AverageMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        try {
            String[] splitted = line.toString().split("\t");

            int placement = Integer.parseInt(splitted[0]);

            if (splitted.length >= 3) {
                String x = splitted[1];
                String y = splitted[2];


                context.write(new IntWritable(placement),
                        new Text(x + "\t" + y));

            }
        } catch (Exception e) {

        }
    }
}
