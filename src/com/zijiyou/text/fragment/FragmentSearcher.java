package com.zijiyou.text.fragment;

import java.net.UnknownHostException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class FragmentSearcher {

	public static void main(String args[]){
		
		Mongo m = null;
		try {
			m = new Mongo("dev.zijiyou.com", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	
		DB db = m.getDB("page");
		char pw[] = "iamgo2010".toCharArray();
		db.authenticate("cheewu", pw);
		
		System.out.println("begin to proscess");
		/**
		 * 获得article列表
		 */
		DBCollection articleColl = db.getCollection("keywordsParagraph");
		BasicDBObject regionQuery = new BasicDBObject();
		regionQuery.put("keyword", "卢浮宫");
		DBCursor articleCur = articleColl.find(regionQuery);
		System.out.println("总文档数:   "+articleCur.count());
		int j=0;
		while (articleCur.hasNext()) {
			j++;
			if(j>50)
				break;
			DBObject document = articleCur.next();
			System.out.println(document);
		}
		
	}
}
