/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q5;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class BookBookRatingReducer extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private ArrayList<Text> listC = new ArrayList<Text>();
        private ArrayList<Text> listD = new ArrayList<Text>();
        private String joinType = null;

        public void setup(Reducer.Context context) {
            // Get the type of join from our configuration
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
            // Clear our lists
            listC.clear();
            listD.clear();
            // iterate through all our values, binning each record based on what
            // it was tagged with. Make sure to remove the tag!
            while (values.iterator().hasNext()) {
                tmp = values.iterator().next();
                System.out.println(Character.toString((char) tmp.charAt(0)));
                if (Character.toString((char) tmp.charAt(0)).equals("C")) {
                    listC.add(new Text(tmp.toString().substring(1)));
                }
                if (Character.toString((char) tmp.charAt(0)).equals("D")) {
                    listD.add(new Text(tmp.toString().substring(1)));
                }
            }
            // Execute our join logic now that the lists are filled

            System.out.println(listD.size());
            executeJoinLogic(context);
        }
         private void executeJoinLogic(Reducer.Context context) throws IOException, InterruptedException {

            if (joinType.equalsIgnoreCase("inner")) {
                if (!listC.isEmpty() && !listD.isEmpty()) {
                    for (Text C : listC) {
                        for (Text D : listD) {
                            context.write(C, D);
                        }
                    }
                }
            } 
         }
}
