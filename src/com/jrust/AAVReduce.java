package com.jrust;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Jonathan Rust on 3/21/16.
 */
public class AAVReduce extends Reducer<Text, Text, Text, Text> {


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> vals = new ArrayList<>();
        for (Text t : values) {
            vals.add(t.toString());
        }

        context.write(key, new Text(vals.toString()));
    }
}
