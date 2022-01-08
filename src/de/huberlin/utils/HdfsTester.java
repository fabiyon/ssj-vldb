/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author fabi
 */
public class HdfsTester {
  public void run() throws Exception {

    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", 
        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    );
    conf.set("fs.file.impl",
        org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );

    conf.addResource(new Path("/home/fabi/researchProjects/textualSimilarity/code/hadoop-2.7.0/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/home/fabi/researchProjects/textualSimilarity/code/hadoop-2.7.0/etc/hadoop/hdfs-site.xml"));

    String dirName = "hdfs://dbis41:9000/user/fier/testJava1";

    //values of hosthdfs:port can be found in the core-site.xml  in the fs.default.name

    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(dirName);
    if (fileSystem.exists(path)) {
        System.out.println("Dir " + dirName + " already exists");
        return;
    }

    // Create directories
    fileSystem.mkdirs(path);

    fileSystem.close();
  }
  
  public static void main(String[] args) throws Exception {
    HdfsTester ht = new HdfsTester();
    ht.run();
  }
}
