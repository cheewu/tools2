package com.zijiyou.text.dict;

import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class WikiTitleDumper {
	
	public static void main(String args[]){
		Set<String> wordSet =new TreeSet<String>();
		Mongo m = null;
		try {
			m = new Mongo("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		DB db = m.getDB("tripfm");

		// 初始化region
		DBCollection wikiColl = db.getCollection("Wikipedia");
		DBCursor wikiCur = wikiColl.find();
		int i=0;
		while (wikiCur.hasNext()) {
			DBObject wikiObject = wikiCur.next();
			String wikititle=wikiObject.get("title").toString();
			if (wikititle.contains("User") ||wikititle.contains("Category") 
					||wikititle.contains("File")|| wikititle.contains("Talk")||
					wikititle.contains("Wikipedia")||
					wikititle.contains("Template") ||
					wikititle.contains("MediaWiki"))
				continue;
			
			//String content=wikiObject.get("content").toString();
			//if(content.contains("#REDIRECT")){
				//continue;
			//	System.out.println(wikititle);
			//}
			int flag=wikititle.indexOf('(');
			if (flag>0){
				wordSet.add(wikititle.substring(0, flag).trim());
				//System.out.println(wikititle.substring(0, flag).trim());
			}else
				wordSet.add(wikititle);
				//System.out.println(wikititle);
			i++;

		}
		wikiCur.close();
		m.close();
		System.out.println("条目总数目:"+ i);

		// 输出分词文件
		FileWriter fw;
		try {
			fw = new FileWriter("wordswiki.txt");
			Iterator<String> itKey = wordSet.iterator();
			while (itKey.hasNext()) {
				String keyword = itKey.next();
				fw.write(keyword + "\n");

			}
			fw.flush();
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
