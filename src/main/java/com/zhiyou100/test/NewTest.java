package com.zhiyou100.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class NewTest {

	public static void main(String[] args) {
		String str = "ncnhromenaecnanary";
		
		char[] strChars = str.toCharArray();

		String orStr = "";

		int index = 0;

		for (char c : strChars) {
			if(c == 'n'){
				System.out.println(index);
				orStr = str.substring(index, index+3);
				if(orStr.equals("nar")){
					System.out.println(index);
				}
			}
			index++;
		}
		
	}

}
