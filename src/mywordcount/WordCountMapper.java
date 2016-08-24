package mywordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jaymiao on 16/8/24.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    final private static LongWritable num = new LongWritable();
    private Text tokenValue = new Text();

    @Override
    protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
        String[] result = text.toString().split("\\s");
        num.set(Integer.parseInt(result[result.length-1]));
        for (int i = 0; i < result.length - 1; i++) {
            tokenValue.set(result[i]);
            context.write(tokenValue, num);
        }
    }
}
