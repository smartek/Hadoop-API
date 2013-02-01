/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hadoopapi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class hadoopclient {
    
    public void getHostnames () throws IOException{
Configuration config = new Configuration();
config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
config.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
config.addResource(new Path("/usr/local/hadoop/conf/mapred-site.xml"));
 
FileSystem fs = FileSystem.get(config);
DistributedFileSystem hdfs = (DistributedFileSystem) fs;
DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
 
String[] names = new String[dataNodeStats.length];
for (int i = 0; i < dataNodeStats.length; i++) {
names[i] = dataNodeStats[i].getHostName();
System.out.println((dataNodeStats[i].getHostName()));
}
}
   public void mkdir(String dir) throws IOException {
Configuration conf = new Configuration();
conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
conf.addResource(new Path("/usr/local/hadoop/conf/mapred-site.xml"));
	 
FileSystem fileSystem = FileSystem.get(conf);
	 
Path path = new Path(dir);
if (fileSystem.exists(path)) {
System.out.println("Dir " + dir + " already exists!");
return;
}
	 
fileSystem.mkdirs(path);
	 
fileSystem.close();
	}
   
   public void readFile(String file) throws IOException {
Configuration conf = new Configuration();
conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
conf.addResource(new Path("/usr/local/hadoop/conf/mapred-site.xml"));

FileSystem fileSystem = FileSystem.get(conf);
	 
	Path path = new Path(file);
	if (!fileSystem.exists(path)) {
	System.out.println("File " + file + " does not exists");
	return;
	}
	 
	FSDataInputStream in = fileSystem.open(path);
	 
	String filename = file.substring(file.lastIndexOf('/') + 1,
	file.length());
	 
	OutputStream out = new BufferedOutputStream(new FileOutputStream(
	new File(filename)));
	 
	byte[] b = new byte[1024];
	int numBytes = 0;
	while ((numBytes = in.read(b)) > 0) {
	out.write(b, 0, numBytes);
	}
	 
	in.close();
	out.close();
	fileSystem.close();
	}
}
