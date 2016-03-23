package com.jrust;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Jonathan Rust on 3/2/16.
 */
public class AuthorCountMap extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("<===>");
        if(split.length == 1) return; /* Line doesn't contain key denoting author, no work to do */

        String author = split[0];

        context.write(new Text("tmpKey"), new Text(author));
    }
}
