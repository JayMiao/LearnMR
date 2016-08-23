package copywithprogress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class CopyWithProgress {
    public static void  main(String args[]) throws Exception {
        String localsrc = args[0];
        String hdfsdir = args[1];

        // input 流
        InputStream in = new BufferedInputStream(new FileInputStream(localsrc));

        // 新建hdfs fs
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsdir), conf);
        // output 流
        OutputStream out = fs.create(new Path(hdfsdir), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
