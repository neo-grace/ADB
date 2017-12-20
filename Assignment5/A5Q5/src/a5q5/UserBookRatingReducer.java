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
public class UserBookRatingReducer extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private ArrayList<Text> listA = new ArrayList<>();
        private ArrayList<Text> listB = new ArrayList<>();
        private String joinType = null;

        public void setup(Context context) {
            // Get the type of join from our configuration
            joinType = context.getConfiguration().get("join.type");
        }

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            listA.clear();
            listB.clear();
            
            while (values.iterator().hasNext()) {
                tmp = values.iterator().next();
                if (Character.toString((char) tmp.charAt(0)).equals("A")) {
                    listA.add(new Text(tmp.toString().substring(1)));
                    //System.out.println("A" + listA);
                }
                if (Character.toString((char) tmp.charAt(0)).equals("B")) {
                    listB.add(new Text(tmp.toString().substring(1)));
                    //System.out.println("B" + listB);

                }
            }
            executeJoinLogic(context);
        }
         private void executeJoinLogic(Context context) throws IOException, InterruptedException {

            if (joinType.equalsIgnoreCase("inner")) {

                if (listA.isEmpty() && listB.isEmpty()) 
                {
                }
                else
                {
                   
                    for(Text A:listA) {
                        for (Text B:listB) {
                            System.out.println(A + " " + B);
                            context.write(A, B);
                        }
                    }
                }
            } 
         }
}
