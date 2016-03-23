package com.jrust;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Jonathan Rust on 3/23/16.
 */
public class CosineSimReduce extends Reducer<Text, Text, Text, Text> {


    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

    }
}
