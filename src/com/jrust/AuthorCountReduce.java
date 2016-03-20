package com.jrust;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by Jonathan Rust on 3/2/16.
 */
public class AuthorCountReduce extends Reducer<Text, Text, Text, Text> {


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashSet<String> authors = new HashSet<>();

        for (Text a : values) {
            if (!authors.contains(a.toString())) {
                authors.add(a.toString());
                context.getCounter(Main.Counters.TOTAL_AUTHORS).increment(1);
            }
        }
    }
}
