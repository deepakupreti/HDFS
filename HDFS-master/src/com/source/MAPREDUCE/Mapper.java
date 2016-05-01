package com.source.MAPREDUCE;

public class Mapper implements IMapper{

	@Override
	public String map(String map) {
		//System.out.println(map);
		String str[] = map.split("<-@->");
		if(str.length > 1 && str[1].contains(str[0])){
			return str[1];
		}
		return "null";
	}

}
