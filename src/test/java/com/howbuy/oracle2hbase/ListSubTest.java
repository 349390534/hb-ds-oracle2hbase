/**
 * 
 */
package com.howbuy.oracle2hbase;

import java.util.ArrayList;
import java.util.List;

/**
 * @author qiankun.li
 *
 */
public class ListSubTest {

	public static void main(String[] args) {
		List<String> list = new ArrayList<String>();
		list.add("1");
		list.add("2");
		list.add("3");
		list.add("4");
		list.add("5");
		int num = list.size();
		int time = num/2;
		int more = num%2;
		for(int i=0;i<time;i++){
			int fromIndex =2*i;
			int toIndex = 2*(i+1);
			System.out.println(list.subList(fromIndex, toIndex));
		}
		if(more>=1){
			int fromIndex = 2*time;
			int toIndex = list.size();
			System.out.println(list.subList(fromIndex, toIndex));
		}
	}
}
