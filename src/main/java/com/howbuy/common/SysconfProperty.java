package com.howbuy.common;

import java.io.IOException;
import java.util.Properties;

public class SysconfProperty {

	private static SysconfProperty sysProperty = new SysconfProperty();
	
	private SysconfProperty() {
	}
	
	public static SysconfProperty getSysProperty(){
		return sysProperty;
	}
	
	public Properties getProperties(){
		Properties properties=null;
		if(null == properties){
			properties = new Properties();
			try {
				//properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("sysconf.properties"));
				properties.load(SysconfProperty.class.getClassLoader().getResourceAsStream("sysconf.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return properties;
	}
}
