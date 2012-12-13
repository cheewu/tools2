package com.zijiyou.wiki;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.mongo.MongoConnector;

import taobe.tec.jcc.JChineseConvertor;

public class WikiGeoAppender {
	
	private static HashMap<String,String> titleIDMap =new HashMap<String,String>();

	public static HashMap<String,String> getDBPediaItem() throws IOException {
		HashMap<String, String> geoItem = new HashMap<String, String>();

		BufferedReader br = new BufferedReader(new FileReader(
				"geo_coordinates_zh.tql"));
		String line = null;
		JChineseConvertor jc=JChineseConvertor.getInstance();
 		Pattern pName = Pattern.compile("resource/(.*)>\\s<");
		Pattern pGeo = Pattern.compile("\"(.*)\"");
		while ((line = br.readLine()) != null){
			String name=null;
			String geo=null;
			if (line.contains("georss")) {
				Matcher match = pName.matcher(line);
				if(match.find())
			        name = jc.t2s(match.group(1));
				Matcher matchgeo = pGeo.matcher(line);
				if(matchgeo.find())
				     geo = matchgeo.group(1);
				geoItem.put(name, geo);
			}
		}
		return geoItem;
	}
	
	
	public static HashMap<String,Float[]> getWikiEn() throws IOException{
		JChineseConvertor jc=JChineseConvertor.getInstance();
		HashMap<String, Float[]> geoItem = new HashMap<String, Float[]>();
		FileWriter fw=new FileWriter("enwiki.txt");
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		
		DBCollection wikiCollection=mgc.getDB().getCollection("WikipediaEn3");
		DBObject query=new BasicDBObject().append("zh",new BasicDBObject().append("$exists", true)); 
		DBCursor wikiCursor=wikiCollection.find(query);
		int j=0;
		while(wikiCursor.hasNext()){
			DBObject wikiitem=wikiCursor.next();
			String name=wikiitem.get("zh").toString();
			String ename=wikiitem.get("title").toString();
			fw.write(name+"   "+ename+"   "+wikiitem.get("_id").toString()+"\n");
			fw.flush();
			if(ename.contains("Bank") && ename.contains("China")&& ename.contains("Tower"))
				System.out.println(ename+"  "+wikiitem);
			
			if(wikiitem.containsField("center")){
				Float[] latilongti=new Float[2];
				latilongti[0]=Float.valueOf(((BasicDBObject)wikiitem.get("center")).get("0").toString());
				latilongti[1]=Float.valueOf(((BasicDBObject)wikiitem.get("center")).get("1").toString());
				geoItem.put(name,latilongti);
			}
		}
		return geoItem;
	}
	
	
	public static HashMap<String,Integer> getMongoWiki() throws IOException{
		JChineseConvertor jc=JChineseConvertor.getInstance();
		HashMap<String, Integer> geoItem = new HashMap<String, Integer>();
		
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		
		DBCollection wikiCollection=mgc.getDB().getCollection("Wikipedia");
		
		//DBObject query=new BasicDBObject().append("center", new BasicDBObject().append("$exists", true));
		//DBObject query=new BasicDBObject().append("title","迪斯科湾");
		DBCursor wikiCursor=wikiCollection.find();
		int j=0;
		while(wikiCursor.hasNext()){
			DBObject wikiitem=wikiCursor.next();
			String name=jc.t2s(wikiitem.get("title").toString());
			if(wikiitem.containsField("center"))
				geoItem.put(name,1);
			else{
				geoItem.put(name, 0);
				titleIDMap.put(name,wikiitem.get("_id").toString());
			}
		}
		return geoItem;
	}
	
	
	public static void WikEn2WikiZh(){
		
		FileWriter fw=null;
		try {
			fw = new FileWriter("missingPOI.txt");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			HashMap<String,Float[]>  enWikiGeo=getWikiEn();
			HashMap<String,Integer> mongoGeo=getMongoWiki();
			
			
			MongoConnector mgc = new MongoConnector("analyzer.properties",
					"mongo_tripfm");
			
			DBCollection wikiCollection=mgc.getDB().getCollection("Wikipedia");
			
			for(Map.Entry<String, Float[]> entry: enWikiGeo.entrySet()){
				String name=entry.getKey();
				Float[] latilongti=entry.getValue();
				if(mongoGeo.containsKey(name)){
					if(mongoGeo.get(name)==null){
						System.out.println("Can't find zh item:"+name);
						continue;
					}
					
					if(mongoGeo.get(name)==0){
						System.out.println("Begin to add geo information for :"+name+" id:  "+titleIDMap.get(name));
						DBObject updateQuery = new BasicDBObject("title", name);
						BasicDBObject updateConetent = new BasicDBObject().append(
								"center", latilongti);
						BasicDBObject newDocument = new BasicDBObject().append("$set",
								updateConetent);
						wikiCollection.update(updateQuery, newDocument,true,true);
					}
					
				}else{
					fw.write(name+"\n");
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	public static void dbpedia2WikiMongo(){
		
		FileWriter fw=null;
		try {
			fw = new FileWriter("missingPOI_eng.txt");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			HashMap<String,Integer> mongoGeo=getMongoWiki();
			HashMap<String,String>  dbpediaGeo=getDBPediaItem();
			
			MongoConnector mgc = new MongoConnector("analyzer.properties",
					"mongo_tripfm");
			DB db = mgc.getDB();
			
			DBCollection wikiCollection=db.getCollection("Wikipedia2");
			
			for(Map.Entry<String, String> entry: dbpediaGeo.entrySet()){
				String name=entry.getKey();
				String[] geo=entry.getValue().split(" ");
				Float[] latilongti=new Float[2];
				latilongti[0]=Float.valueOf(geo[1]);
				latilongti[1]=Float.valueOf(geo[0]);
				
				if(mongoGeo.containsKey(name)){
					if(mongoGeo.get(name)==0){
						System.out.println("Begin to add geo information for :"+name);
						DBObject updateQuery = new BasicDBObject("title", name);
						BasicDBObject updateConetent = new BasicDBObject().append(
								"center", latilongti);
						BasicDBObject newDocument = new BasicDBObject().append("$set",
								updateConetent);
						wikiCollection.update(updateQuery, newDocument);
					}
				}else{
					fw.write(name+"\n");
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void main(String args[]) throws IOException {
		WikEn2WikiZh();
        //getMongoWiki();

		
	}

}
