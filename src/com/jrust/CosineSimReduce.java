package com.jrust;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Created by Jonathan Rust on 3/23/16.
 */
public class CosineSimReduce extends Reducer<Text, Text, Text, Text> {

    /**
     *
     * @param key - Unknown author AAV
     * @param values - All known AAV's with format Author\t[word_1\tfidf_1,......, word_n\tfidf_n]
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String mysteryAAV = "";
        ArrayList<String> knownList = new ArrayList<>();
        for (Text t : values) {
            if(t.toString().substring(0,14).equals("Mystery Author")) {
                mysteryAAV = t.toString().split("\t\\[")[1].replaceAll(",", "");
                mysteryAAV = mysteryAAV.substring(1, mysteryAAV.length() - 1);
            } else {
                knownList.add(t.toString());
            }

        }

        StringTokenizer maavTokenizer = new StringTokenizer(mysteryAAV);
        DecimalFormat df = new DecimalFormat("0.000000000000000");
        HashMap<String, Double> targetAAV = new HashMap<>();
        while (maavTokenizer.hasMoreTokens()) {
            targetAAV.put(maavTokenizer.nextToken(), Double.parseDouble(maavTokenizer.nextToken()));
        }

        for (String known : knownList) {
            HashMap<String, Double> knownAAV = new HashMap<>();
            String[] split = known.toString().split("\t\\[");
            String author = split[0];
            String inputArr = split[1].replaceAll(",", "");
            inputArr = inputArr.substring(0, inputArr.length() - 1);
            StringTokenizer inputTokenizer = new StringTokenizer(inputArr);

            while (inputTokenizer.hasMoreTokens()) {
                knownAAV.put(inputTokenizer.nextToken(), Double.parseDouble(inputTokenizer.nextToken()));
            }

            double dotProduct = 0.0;
            double target_mag = 0.0;
            for (HashMap.Entry<String, Double> entry : targetAAV.entrySet()) {
                if (knownAAV.containsKey(entry.getKey())) {
                    dotProduct += entry.getValue() * knownAAV.get(entry.getKey());
                }
                target_mag += entry.getValue() * entry.getValue();
            }

            double input_mag = 0.0;
            for (HashMap.Entry<String, Double> entry : knownAAV.entrySet()) {
                input_mag += entry.getValue() * entry.getValue();
            }

            Double cosineSimilarity = dotProduct / (Math.sqrt(target_mag) * Math.sqrt(input_mag));

            context.write(new Text(author), new Text(df.format(cosineSimilarity)));
        }
    }
}
