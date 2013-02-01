package com.mycompany.hadoopapi;

import java.io.IOException;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        //System.out.println( "Hello World!" );
        try
        {
            System.out.println("Started.");
        hadoopclient hclient = new hadoopclient();
        hclient.getHostnames();
        hclient.mkdir("hclient");
        hclient.mkdir("/pig-lecture/sample.txt");
        }
        catch(IOException ex)
        {
            System.out.println( "Exception:" + ex );
        }
    }
    
}
