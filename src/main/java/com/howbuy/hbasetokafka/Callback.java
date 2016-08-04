package com.howbuy.hbasetokafka;

import java.util.List;
import java.util.Map;

/**
 * 数据回调处理类
 * @author yichao.song
 *
 */
public interface Callback {

	void process(List<Map<String,String>> param);
}
