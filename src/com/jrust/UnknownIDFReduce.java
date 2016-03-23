package com.jrust;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Jonathan Rust on 3/23/16.
 */
public class UnknownIDFReduce extends Reducer<Text, Text, Text, Text> {

    /**
     * Outputs <"unknown", term + tfidf> which gets fed into AAVMap
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    }
}
