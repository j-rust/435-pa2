package com.jrust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by Jonathan Rust on 3/8/16.
 */
public class IDFReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    /**
     * @args key - term
     * @args values - [Author_0 + TF_0, Author_1 + TF_1, ..., Author_N + TF_N
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long total_authors = Long.parseLong(conf.get("authors"));
        HashSet<String> authors = new HashSet<>();
        for (Text t : values) {
            String author = t.toString().split("\\s")[0];
            authors.add(author);
        }
        Integer author_count = authors.size();
        Double idf = Math.log(total_authors / author_count);

        for (Text t : values) {
            String author = t.toString().split("\\s")[0];
            Double tf = Double.parseDouble(t.toString().split("\\s")[1]);
            Double tfidf = tf*idf;
            context.write(new Text(key.toString() + " " + author), new Text(tfidf.toString()));
        }

    }
}
