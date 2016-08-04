/**
 * 
 */
package com.howbuy.oracle2hbase;

import java.io.IOException;


/**
 * @author qiankun.li
 *
 */
public class Sqoop1Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ProcessBuilder builder = new ProcessBuilder("sqoop-import","import","--append", "--connect", "jdbc:oracle:thin:@192.168.220.103:1521:orac10g", "--username",
	                "cust1", "--password", "cust1", "--m","1","--table", "CM_CUSTLABLE","--columns",
	                "id,code", "--hbase-create-table", "--hbase-table", "custlabel","--hbase-row-key","id",
	                " --column-family","custlabel");
		 
		
		 try {
			    Process p = builder.start();
			    if (p.waitFor() != 0) {   
			        System.out.println("Error: sqoop-export failed.");
			    }
			} catch (IOException e) {
			    e.printStackTrace();
			} catch (InterruptedException e) {
			    e.printStackTrace();
			}

		/*String url = "http://192.168.220.154:12000/sqoop/";
		SqoopClient client = new SqoopClient(url);*/
		
	}

}
