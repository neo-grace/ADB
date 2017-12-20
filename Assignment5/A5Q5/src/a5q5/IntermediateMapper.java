/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class IntermediateMapper extends Mapper<Object,Text,Text,Text>{
    private Text outkey = new Text();
    private Text outvalue = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String str[] = value.toString().split("\\t");
        if(str[0].equals("ISBN"))
            return;
        else
        {
            String isbn = str[4];
            outkey.set(isbn);
            outvalue.set("C" + value.toString());
            context.write(outkey, outvalue);
        }
    }
}
