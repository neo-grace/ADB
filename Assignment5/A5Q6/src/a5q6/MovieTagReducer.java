/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q6;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author neola
 */
public class MovieTagReducer extends Reducer<Text,Text,Text,NullWritable>{
    private ArrayList<String> tags = new ArrayList<String>();
    private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    private String movie = null;
    
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
    {
        movie = null;
        tags.clear();
        
        for(Text t:values)
        {
            if(t.charAt(0) == 'M'){
                movie = t.toString().substring(1,t.toString().length()).trim();
                System.out.println(movie);
            }
            else
            {
                tags.add(t.toString().substring(1,t.toString().length()).trim());
               
            }
        }
        
        if(movie!=null){
            
            try {
                String movieWithTags = nestElements(movie,tags);
                
                System.out.println(movieWithTags);
                context.write(new Text(movieWithTags), NullWritable.get());
            } catch (ParserConfigurationException ex) {
                Logger.getLogger(MovieTagReducer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SAXException ex) {
                Logger.getLogger(MovieTagReducer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TransformerException ex) {
                Logger.getLogger(MovieTagReducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private String nestElements(String movie, ArrayList<String> tags) throws ParserConfigurationException, IOException, SAXException, TransformerException {
        DocumentBuilder bldr = dbf.newDocumentBuilder();
        Document doc = bldr.newDocument();
      
        
        Element addMovieEl = doc.createElement("movie");
        Attr attr = doc.createAttribute("name");
        attr.setValue(movie);
        addMovieEl.setAttributeNode(attr);
        doc.appendChild(addMovieEl);
        //copyAttributesToElement(movieEl.getAttributes(),addMovieEl);
        
        for(String tagXml : tags){
            //Element tagEl = tagXml;
            Element addTagEl = doc.createElement("tag");
            addTagEl.appendChild(doc.createTextNode(tagXml));
            addMovieEl.appendChild(addTagEl);

        }
       
        return transformDocumentToString(doc);
        
    }

    /*private Element getXmlElementFromString(String xml) throws IOException, SAXException, ParserConfigurationException {
        DocumentBuilder bldr = dbf.newDocumentBuilder();
        return bldr.parse(new InputSource(new StringReader(xml))).getDocumentElement();
    }

    private void copyAttributesToElement(NamedNodeMap attributes, Element addTagEl) {
        for(int i=0;i<attributes.getLength();++i){
            Attr toCopy = (Attr)attributes.item(i);
            addTagEl.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }*/

    private String transformDocumentToString(Document doc) throws TransformerConfigurationException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.getBuffer().toString().replaceAll("\n|\r", "");
    }
}
