package 老师给的示例;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *complie HDFSTest.java
 *
 * javac HDFSTest.java 
 *
 *execute HDFSTest.java
 *
 * java HDFSTest  
 * 
 */

public class HDFSTest {

    public static void main(String[] args) throws IOException, URISyntaxException{
        String file= "hdfs://localhost:9000/hw1/README.txt";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        while ((s=in.readLine())!=null) {
             System.out.println(s);
        }

        in.close();

        fs.close();
    }
}
