package com.jrust;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashMap;

/**
 * Created by Jonathan Rust on 3/23/16.
 */
public class MysteryIDFReduce extends Reducer<Text, Text, Text, Text> {

    private Double DEFAULT_IDF = Math.log(1710.0) / Math.log(2.0);
    private HashMap<String, Double> IDF_CACHE;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        IDF_CACHE = new HashMap<>();

        URI[] uris = context.getCacheFiles();

        File file = new File("part-r-00000");

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        while (line != null) {
            String[] split = line.split("\t");
            IDF_CACHE.putIfAbsent(split[0], Double.parseDouble(split[1]));
            line = reader.readLine();
        }

    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String term = key.toString();
        Double tf = Double.parseDouble(values.iterator().next().toString());

        Double idf;

        if (IDF_CACHE.containsKey(term)) {
            idf = IDF_CACHE.get(term);
        } else {
            idf = DEFAULT_IDF;
        }
        Double tfidf = tf * idf;
        DecimalFormat df = new DecimalFormat("0.000000000000000");

        context.write(new Text("Mystery Author"), new Text(term + "\t" + df.format(tfidf)));


    }

}
