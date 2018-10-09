package big.data.hse.hw.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sievmi on 09.10.18
 */
public class AggregateFileMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        try {
            String[] splitted = line.toString().split(",");

            if (splitted.length >= 15) {
                String matchId = splitted[2];
                String playerName = splitted[11];
                int placement = Integer.parseInt(splitted[14]);

                if (placement >= 2 && placement <= 4) {
                    context.write(new Text(matchId + "#?#" + playerName),
                            new Text(matchId + "\t" + playerName + "\t" + placement));
                }
            }
        } catch (Exception e) {

        }
    }
}

