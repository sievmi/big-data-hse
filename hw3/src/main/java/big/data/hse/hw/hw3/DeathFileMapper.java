package big.data.hse.hw.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sievmi on 09.10.18
 */
public class DeathFileMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        try {
            String[] splitted = line.toString().split(",");

            if (splitted.length >= 10) {
                String matchId = splitted[5];
                String playerName = splitted[7];
                String x = splitted[8];
                String y = splitted[9];


                context.write(new Text(matchId + "#?#" + playerName),
                        new Text(matchId + "\t" + playerName + "\t" + x + "\t" + y));
            }
        } catch (Exception e) {

        }
    }
}
