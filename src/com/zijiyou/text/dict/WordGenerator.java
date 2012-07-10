package com.zijiyou.text.dict;

//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeMap;
//
//import org.apache.log4j.Logger;
//
//
//import com.mongodb.BasicDBObject;
//import com.mongodb.DB;
//import com.mongodb.DBCollection;
//import com.mongodb.DBCursor;
//import com.mongodb.DBObject;
//import com.zijiyou.mongo.MongoConnector;

public class WordGenerator {
//	
//	public static final Integer CAT_COUNTRY=1;
//	public final static Integer CAT_PROVINCE=2;
//	public final static Integer CAT_DESTINATION=3;
//	
//	public final static Integer CAT_POI=4;
//	
//	public final static Integer CAT_NOTE=5;
//	public final static Integer CAT_FOOD=6;
//	public final static Integer CAT_ITEM=7;
//	
//	public final static Integer CAT_OTHER=99;
//	
//	private static final Logger LOG = Logger.getLogger(WordGenerator.class);
//	
//	private static Map<String, Integer>  keywordMap=new TreeMap<String,Integer>();    //keyword->category(country,province,destination,poi,food,item,note)
//	private static Map<String, String[]> regionMap=new HashMap<String, String[]>(); //regionID-->regionName,category
//	private static Map<String, String[]> poiMap =new HashMap<String, String[]>();    //keyword-->poiName,poiID,regionName
//	private static Set<String> kwset=new HashSet<String>();
//	
//	private static boolean isInitialized=false;
//	
////	public static Integer getKeywordInfo(String kw){
////		if(!isInitialized)
////			initialize();
////		return keywordMap.get(kw);
////	
////	}
//	
////	public static String getKeywordCategory(String kw){
////		Integer category=getKeywordInfo(kw);
////		if (category==null ){
////			LOG.error("Can't find category for keyword:"+kw);
////			return "null";
////		}
////		switch (category){
////		case 1:
////			return "country";
////		case 2:
////			return "province";
////		case 3: 
////			return "destination";
////		case 4:
////			return "poi";
////		case 5:
////			return "note";
////		case 6:
////			return "food";
////		case 7 :
////			return "item";
////		case 99:
////			return "other";
////		}
////		return "null";
////	}
//	
//	public  static String[] getRegionInfo(String regionName){
//		if(!isInitialized)
//			initialize();
//		return regionMap.get(regionName);
//	}
//	
//	
//	public static String[] getPOIInfo(String poiName){
//		if(!isInitialized)
//			initialize();
//		return poiMap.get(poiName);
//	}
//	
//	public static  void initialize(){
//		if(isInitialized)
//			return;
//		
//		FileWriter fw = null;
//		try {
//			fw = new FileWriter("duplicatepoit.txt");
//		}catch (Exception e){
//			e.printStackTrace();
//		}
//		
//		DB db=MongoConnector.getDBByProperties("analyzer.properties", "mongo_tripfm");
//		
//		// 初始化region
//		DBCollection regionColl = db.getCollection("Region");
//		BasicDBObject regionQuery = new BasicDBObject();
//		String regioncategories[] = new String[] { "country", "province", "destination" };
//		regionQuery.put("category", new BasicDBObject("$in", regioncategories));
//		DBCursor regionCur = regionColl.find();
//		
//		while (regionCur.hasNext()) {
//			DBObject regionObject = regionCur.next();
//			
//			if(!regionObject.containsField("name")){
//				System.out.println(regionObject);
//				continue;
//			}
//			
//			String regionName=regionObject.get("name").toString(); 
//
//			if (regionName== null||!isChinese(regionName.charAt(0)))
//				continue;
//			
//			String[] regionInfo = new String[2];
//			Integer regionCat=CAT_OTHER;
//			regionInfo[0] = regionObject.get("name").toString();
//			regionInfo[1] = regionObject.get("category").toString();
//			if(regionInfo[1].equals("country"))
//				regionCat=CAT_COUNTRY;
//			
//			if(regionInfo[1].equals("province"))
//				regionCat=CAT_PROVINCE;
//			
//			if(regionInfo[1].equals("destination"))
//				regionCat=CAT_DESTINATION;
//			
//			keywordMap.put(regionObject.get("name").toString().trim(), regionCat);
//			regionMap.put(regionObject.get("_id").toString().trim(), regionInfo);
//		}
//		
//		regionCur.close();
//		
//		// 初始化POI
//		DBCollection poiColl = db.getCollection("POI");
//		BasicDBObject query = new BasicDBObject();
//		query.put("category", "attraction");
//		DBCursor poiCur = poiColl.find(query);
//		while (poiCur.hasNext()) {
//			DBObject poiObject = poiCur.next();
//			if (poiObject.get("name") == null)
//				continue;
//			String[] poiInfo = new String[3];
//			poiInfo[0] = poiObject.get("name").toString();
//			poiInfo[1] = poiObject.get("_id").toString();
//			
//			if (poiObject.get("regionId") != null){
//				if(regionMap.get(poiObject.get("regionId").toString())!=null) 
//						poiInfo[2] = regionMap.get(poiObject.get("regionId").toString())[0];
//			}
//			
//			String[] kws = null;
//			if ((poiObject.get("keyword")) != null) {
//				kws = poiObject.get("keyword").toString().split(",");
//				for (int j = 0; j < kws.length; j++) {
//					keywordMap.put(kws[j].trim(), CAT_POI);
//					if (poiMap.containsKey(kws[j]) ){
//						/*
//						 * 两个景点的关键字一样 且不在一个region下面 则忽略
//						 */
//						try {
//							fw.write(kws[j]+"   "+poiObject.get("name").toString()+"   "+poiObject.get("_id").toString()+"\n");
//							fw.flush();
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//						poiMap.put(kws[j], null);
//						//System.out.println("Duplicate Keyword:  "+kws[j]+"   "+poiObject.get("_id"));
//					} else
//						poiMap.put(kws[j], poiInfo);
//				}
//			} else {
//				keywordMap.put(poiObject.get("name").toString().trim(), CAT_POI);
//				poiMap.put(poiObject.get("name").toString().trim(), poiInfo);
//			}
//		}
//		poiCur.close();
//
//		//初始化keyword
//		DBCollection keywordColl = db.getCollection("Keyword");
//		query = new BasicDBObject();
//		String categories[] = new String[] { "food", "note", "item" };
//		query.put("category", new BasicDBObject("$in", categories));
//		query.put("is_del", false);
//		DBCursor keywordCur = keywordColl.find(query);
//		while (keywordCur.hasNext()) {
//			DBObject kwObject = keywordCur.next();
//			String kw = kwObject.get("keyword").toString();
//			if (kw == null)
//				continue;
//			Integer kwCat=0;
//			String catergory=kwObject.get("category").toString();
//			if(catergory.equals("food"))
//				kwCat=CAT_FOOD;
//			if(catergory.equals("note"))
//				kwCat=CAT_NOTE;
//			if(catergory.equals("item"))
//				kwCat=CAT_ITEM;
//			
//			
//			keywordMap.put(kwObject.get("keyword").toString().trim(),kwCat );
//			kwset.add(kwObject.get("keyword").toString().trim());
//		}
//		keywordCur.close();
//		isInitialized=true;
//	}
//	
//	public static boolean isChinese(char c) {  
//	     Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);  
//	     if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS  
//	            || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS  
//	            || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A  
//	            || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION  
//	            || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION  
//	            || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {  
//	        return true;  
//	    }  
//	    return false;  
//	}
//	
//	
//	public static void main(String args[]) {
//		initialize();
//		// 输出分词文件
//		FileWriter fw;
//		try {
//			fw = new FileWriter("kw.txt");
//			Iterator<String> itKey = keywordMap.keySet().iterator();
//			while (itKey.hasNext()) {
//				String keyword = itKey.next();
//				fw.write(keyword+"\n");
//			}
//			fw.flush();
//			fw.close();
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//	}
}