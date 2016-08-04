package com.howbuy.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;


public class JsonParse {
	 
	
	
	/**
	 * 集合转换为json
	 * @param object
	 * @return
	 */
	public static String arrayToJsonStr(Object  object){
		JsonConfig config = new JsonConfig();
		JSONArray array = JSONArray.fromObject(object,config);
		return array.toString();
	}
	
	/**
	 * 对象转换为json
	 * @param object
	 * @return
	 */
	public static String objToJsonStr(Object  object){
		JsonConfig config = new JsonConfig();
		JSONObject jsonObject = JSONObject.fromObject(object,config);
		return jsonObject.toString();
	}
	
	
	public static <T> void writeJson(HttpServletResponse response, T t) {
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json; charset=utf-8");
		String json = null;
		if (t instanceof List) {
			json = JsonParse.arrayToJsonStr(t);
		} else {
			json = JsonParse.objToJsonStr(t);
		}

		PrintWriter out = null;
		try {
			out = response.getWriter();
			out.write(json);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != out) {
				out.close();

			}
		}
	}
}
