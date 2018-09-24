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
        String[] words = line.toString().toLowerCase().split("[^\\p{L}]");
        IntWritable textLength = new IntWritable(words.length);
        /*for (String word : words) {
            context.write(new Text(word), textLength);
        }*/
        if (words.length > 0) context.write(new Text(words[0]), textLength);
    }
}
