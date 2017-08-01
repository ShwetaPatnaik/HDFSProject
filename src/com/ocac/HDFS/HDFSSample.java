package com.ocac.HDFS;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSSample {
	
	public static final String inputfile="hdfsinput.txt";
	public static final String inputmsg="count the amount of words in the sentence";
	
	public static void main(String[] args) throws IOException {
		// create a default hadoop configuration
	Configuration config=new Configuration();
		//created config to hdfs 
	FileSystem fs=FileSystem.get(config);
			
		//specifies a new file in hdfs
	Path FilenamePath=new Path(inputfile);
	try{
		//if the file already exists delete it
		if(fs.exists(FilenamePath)){
		//remove the file
			fs.delete(FilenamePath);
	}
		//FSOutputStream to write the inputmsg into the HDFS file
	FSDataOutputStream fin=fs.create(FilenamePath);
	fin.writeUTF(inputmsg);
	fin.close();
	
	FSDataInputStream fout=fs.open(FilenamePath);
	String msgIn=fout.readUTF();
	System.out.println(msgIn);
	fout.close();
	}
	catch(IOException ioe){
		System.err.println("IOException during operation"+ioe.toString());
		System.exit(1);
	}
	}
}
