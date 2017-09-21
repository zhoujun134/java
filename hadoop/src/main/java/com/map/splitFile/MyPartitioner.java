package com.map.splitFile;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner<KEY, VALUE> extends Partitioner<KEY,VALUE> {

private static HashMap<String,Integer> areaMap = new HashMap<>();
	
	static{
		areaMap.put("ERROR", 0);
		areaMap.put("INFO", 1);
		areaMap.put("WARN", 2);
		areaMap.put("OTHER", 3);
	}
	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {
		
		int areaCoder  = areaMap.get(key.toString()) == null ? 3:areaMap.get(key.toString());

		return areaCoder;
	}

}
