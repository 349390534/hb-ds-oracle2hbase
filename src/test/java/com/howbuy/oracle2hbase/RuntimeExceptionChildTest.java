/**
 * 
 */
package com.howbuy.oracle2hbase;

/**
 * @author qiankun.li
 *
 */
public class RuntimeExceptionChildTest{

	void test1()throws RuntimeException{
		int a=1;
		throw new RuntimeExceptionChild1();
	}
	void test2()throws RuntimeException{
		int a=1;
		throw new RuntimeExceptionChild2();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RuntimeExceptionChildTest test = new RuntimeExceptionChildTest();
		try {
			test.test1();
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getClass());
		}
		try {
			test.test2();
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getClass());
		}
		
	}

}
