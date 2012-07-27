package com.zijiyou.text.dict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.common.MapUtil;
import com.zijiyou.mongo.MongoConnector;

public class DictFileUtil {

	public static Set<String> getDict(String file, String split) {
		Set<String> wordSet = new HashSet<String>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = in.readLine()) != null) {
				String[] segs = line.split(split);
				if (segs[0].length() > 0)
					wordSet.add(segs[0]);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return wordSet;
	}

	public static Map<String, Integer> getDictMap(String file,String split) {
		Map<String, Integer> wordMap = new HashMap<String, Integer>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = in.readLine()) != null) {
				String[] segs = line.split(split);
				if (segs.length < 2) {
					System.out.println(line);
					continue;
				}
				wordMap.put(segs[0], Integer.parseInt(segs[1]));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return MapUtil.sortByValue(wordMap);
	}

	public static void removeRareKeyword() {

		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DBCollection kwColl = mgc.db.getCollection("Keyword");
		Map<String, Integer> allMap = getDictMap("/Users/cheewu/Desktop/travel_tf.txt"," ");
		allMap = MapUtil.sortByValue(allMap);
		DBCursor kwcur = kwColl.find();

		while (kwcur.hasNext()) {

			DBObject kwdoc = kwcur.next();
			String name = kwdoc.get("keyword").toString();
			if (!allMap.containsKey(name) || allMap.get(name) < 50) {
				System.out.println(name);
				DBObject updateQuery = new BasicDBObject("keyword", name);
				BasicDBObject updateConetent = new BasicDBObject().append(
						"is_del", true);

				BasicDBObject newDocument = new BasicDBObject().append("$set",
						updateConetent);
				kwColl.update(updateQuery, newDocument);

			}

		}

	}

	
	private static void updateKeywordTF(String tfFile){
		
		Map<String,Integer> tfMap=getDictMap(tfFile,",");
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DBCollection kwColl = mgc.db.getCollection("keywordMap");
		DBCursor kwcur=kwColl.find();
		Set<String> kwset=new HashSet<String>();

		while(kwcur.hasNext()){
			String kw=kwcur.next().get("name").toString();
			if(kwset.contains(kw)){
				System.out.println("Duplicate kw in db"+kw);
				
			}else
				kwset.add(kw);
		}
		
		for(Map.Entry<String, Integer> entry: tfMap.entrySet()){
			String key=entry.getKey();
			if(!kwset.contains(key)){
				System.out.println("Not exist: "+ key);
				continue;
			}
				
			Integer count=entry.getValue();
			Double idf=Math.log(5000000/count);
			System.out.println(key+"  "+idf);
			BasicDBObject dbo=new BasicDBObject().append("$set",new BasicDBObject("idf",idf) );
			kwColl.update(new BasicDBObject().append("name", key), dbo);
		}
		
	}
	
	
	
	public static void analyzerResult(String file) throws IOException {
		Set<String> DictMap = getDict(file, " ");
		FileWriter fw = new FileWriter("tmp.txt");
		for (String kw : DictMap) {

			String firstchar = kw.substring(0, 1);
			if (firstchar.getBytes().length == 1) {
				if (kw.startsWith("") && !kw.startsWith("FOOD")) {
					System.out.println(kw.substring(1));
					// fw.write(kw.substring(1) + "\n");
					// fw.flush();
				}
			}

			if (kw.startsWith("REGION") || firstchar.equals("G")
					|| firstchar.equals("K") || kw.startsWith("PEOPLE")
					|| kw.startsWith("POI") || kw.startsWith("ITEM")
					|| kw.startsWith("ORG") || kw.startsWith("FOOD")
					|| kw.startsWith("PROD") || kw.startsWith("NOTE")
					|| kw.startsWith("HOTEL") || kw.startsWith("I")
					|| kw.startsWith("F"))
				continue;

		}
		fw.close();
	}

	public static void checkKeyword() {

		Map<String, DBObject> kwMap = new HashMap<String, DBObject>();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DBCollection kwColl = mgc.db.getCollection("Keyword");
		BasicDBObject query = new BasicDBObject().append("is_del", false);
		DBCursor dbs = kwColl.find();
		while (dbs.hasNext()) {
			DBObject dbo = dbs.next();
			String name = dbo.get("keyword").toString();
			if (!kwMap.containsKey(name)) {
				kwMap.put(name, dbo);
			} else {
				System.out.println("Duplicate: \norg:" + kwMap.get(name)
						+ "\nnew:" + dbo.toString() + "\n");
			}

		}
		mgc.close();
	}

	public static void appendDict(String filename, String category) {
		Map<String, DBObject> kwMap = new HashMap<String, DBObject>();
		
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DBCollection kwColl = mgc.db.getCollection("Keyword");

		BasicDBObject query = new BasicDBObject().append("is_del", false);
		DBCursor dbs = kwColl.find();
		while (dbs.hasNext()) {
			DBObject dbo = dbs.next();
			String name = dbo.get("keyword").toString();
			if (!kwMap.containsKey(name)) {
				kwMap.put(name, dbo);
			} 
		}

		Set<String> kwset = getDict(filename, " ");
		for (String kw : kwset) {
			if(kwMap.containsKey(kw)){
				System.out.println("duplicate:"+kw);
				continue;
				
			}
			BasicDBObject dbo = new BasicDBObject();
			dbo.append("category", category).append("keyword", kw)
					.append("wiki", kw).append("type", "cn")
					.append("is_del", false).append("firstchar", "3");
			kwColl.insert(dbo);
			System.out.println(kw);

			//  凯尔特人

		}
		mgc.close();
	}

	public static void main(String args[]) {

		//System.out.println(Math.log(2));
		System.out.println("Usage: DicttFileUtil <tf file> " );
		try {
			updateKeywordTF("travel_tf.txt");
			// checkKeyword();
			// analyzerResult("append_wiki.txt");
			//appendDict("append_prod.txt", "product");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
