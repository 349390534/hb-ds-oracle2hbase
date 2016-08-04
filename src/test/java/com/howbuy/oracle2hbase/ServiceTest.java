package com.howbuy.oracle2hbase;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServiceTest {

	public static void main(String[] args) {
		/*ExecutorService ex = Executors.newFixedThreadPool(1);
		ex.execute(new Runnable() {
			@Override
			public void run() {
				while(true){
					System.out.println("===========");
				}
			}
		});
		ex.shutdown();
*/
		
		StringBuffer s = new StringBuffer("1000440963");
		System.out.println(s.reverse().toString());
	}

}
