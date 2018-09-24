package big.data.hse.hw.hw2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by sievmi on 23.09.18
 */

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        String[] splitted = line.toString().split("\t", 3);
        if (splitted.length == 3) {
            String docId = splitted[0];
            String title = splitted[1];
            String[] words = splitted[2].toLowerCase().split("[^\\p{L}]");

            IntWritable textLength = new IntWritable(words.length);
            for (String word : words) {
                context.write(new Text(word), textLength);
            }
        }

    }
}
