package com.jrust;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Jonathan Rust on 3/24/16.
 */
public class SimilaritySortCombiner extends Reducer<Text, Text, Text, Text> {


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> best_matches = new ArrayList<>();
        HashMap<String, Double> sim_map = new HashMap<>();
        for (Text t : values) {
            String[] split = t.toString().split("\t");
            String author = split[0];
            Double sim = Double.parseDouble(split[1]);
            sim_map.put(author, sim);
        }

        int max = 10;
        if(sim_map.size() < 10) {
            max = sim_map.size();
        }
        for(int i = 0; i < max; i++) {
            Double max_sim = -2.0;
            String max_author = "";
            for (HashMap.Entry<String, Double> entry : sim_map.entrySet()) {
                if (entry.getValue() > max_sim) {
                    max_sim = entry.getValue();
                    max_author = entry.getKey();
                }
            }
            best_matches.add(max_author + "\t" + max_sim);
            sim_map.remove(max_author);
        }

        for (String pair : best_matches) {
            context.write(key, new Text(pair));
        }
    }

}
