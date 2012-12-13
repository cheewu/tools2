package com.zijiyou.wiki;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.ios.SQLite;
import com.zijiyou.mongo.MongoConnector;

public class WikiChecker {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//checkDuplicateWiki();
		checkWikiRedirect();
	}

	public static void checkWikiRedirect(){
		MongoConnector mgc=new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db =mgc.getDB();
		DBCollection poiCollection = db.getCollection("Wikipedia");
		DBCursor wikiCursor = poiCollection.find();
		Set<String> pendingDelete=new HashSet<String>();
		
		while (wikiCursor.hasNext()) {
			DBObject dbo=wikiCursor.next();
			String content=dbo.get("content").toString();
			String id=dbo.get("_id").toString();
			
			if(content==null ||content.startsWith("#")){
				poiCollection.remove(dbo);
				System.out.println("Removed "+dbo.get("title").toString()+"   "+id+"  "+content);
			}
		}
		
		
	}
	
	
	public static void checkDuplicateWiki(){

		MongoConnector mgc=new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db =mgc.getDB();
		DBCollection poiCollection = db.getCollection("POI");
		DBObject query = new BasicDBObject().append("regionId", new ObjectId(
				"4e8c091fd0c2ff482300031d"));
		DBCursor poiCursor = poiCollection.find(query);
		HashMap<String,String>  wikiSet=new HashMap<String,String>();
		HashMap<String,String>  poiWikiSet=new HashMap<String,String>();
		Set<String> pendingDelete=new HashSet<String>();
		
		while (poiCursor.hasNext()) {
			DBObject dbo=poiCursor.next();
			if(dbo.containsField("is_del")&&dbo.get("is_del").toString().equals("true"))
				continue;
			String category=dbo.get("category").toString();
			String poiid=dbo.get("_id").toString();
			if(category.equals("wikipedia")){
				if(wikiSet.containsKey(dbo.get("name").toString())){
					pendingDelete.add(poiid);
					System.out.println("Delete " +poiid+"  "+dbo.get("name").toString()+"  "+wikiSet.get(dbo.get("name").toString()));
				}
				else{
					wikiSet.put(dbo.get("name").toString(),poiid);
				}
				
			}else{
				if(dbo.containsField("wikititle")&&!dbo.get("wikititle").equals("")){
					poiWikiSet.put(dbo.get("wikititle").toString(), poiid);
				}
			}
		}
		
		for(String wiki:wikiSet.keySet()){
			if(poiWikiSet.containsKey(wiki)){
				System.out.println("delete "+wiki+"  "+wikiSet.get(wiki));
				pendingDelete.add(wikiSet.get(wiki));
			}
		}
		
		for(String id:pendingDelete){
			
			DBObject qdel=new BasicDBObject();
			qdel.put("_id", new ObjectId(id));
			poiCollection.remove(qdel);
		}
		
		mgc.close();
		
	}
	
	
	
	public static void checkPOIWiki() throws IOException{
		
		MongoConnector mgc=new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db =mgc.getDB();
		DBCollection poiCollection = db.getCollection("POI");
		DBObject query = new BasicDBObject().append("regionId", new ObjectId(
				"4e8c091fd0c2ff482300031d"));
		DBCursor poiCursor = poiCollection.find(query);
		Map<String,String> poiWikiMap=new HashMap<String,String>();
		Set<String> wikiItem=new HashSet<String>();
		while (poiCursor.hasNext()) {
			DBObject dbo = poiCursor.next();
			String id = dbo.get("_id").toString();
			String category=dbo.get("category").toString();
			
			if(!category.equals("wikipedia")&& dbo.get("wikititile")!=null ){
				poiWikiMap.put(dbo.get("wikititile").toString(), id);
			}
			
			if(category.equals("wikipedia"))
				wikiItem.add(dbo.get("name").toString());
		} 

		for(String wiki : wikiItem){
			String content=SQLite.getWikiContent(wiki);
			if(content.startsWith("#REDIRECT"))
				System.out.println("WIKI REDIRECT:"+wiki+":"+content);
		}

		for(String wiki: poiWikiMap.keySet()){
			
			String content=SQLite.getWikiContent(wiki);
			if(content.startsWith("#REDIRECT"))
				System.out.println("POI with redirect wiki:"+ wiki+":"+content);
		}
		
		poiCursor.close();
		mgc.close();
		
	}

}
