package mywordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by jaymiao on 16/8/24.
 */
public class WordCountMapperTest {
    @Test

    public void testMapper() throws IOException {
        MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
        mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        WordCountMapper mapper = new WordCountMapper();

        mapDriver.setMapper(mapper);

        mapDriver.withInput(new LongWritable(1), new Text("cat cat dog 5"))
                .withOutput(new Text("cat"), new LongWritable(5))
                .withOutput(new Text("cat"), new LongWritable(5))
                .withOutput(new Text("dog"), new LongWritable(5))
                .runTest();

    }
}