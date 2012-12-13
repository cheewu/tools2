package com.zijiyou.wiki;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import taobe.tec.jcc.JChineseConvertor;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.mongo.MongoConnector;

public class Wiki2POI {

	public static HashMap<String, Float[]> getWikipediaGeo() throws IOException {
		JChineseConvertor jc = JChineseConvertor.getInstance();
		HashMap<String, Float[]> geoItem = new HashMap<String, Float[]>();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db = mgc.getDB();
		DBCollection wikiCollection = db.getCollection("Wikipedia");
		DBObject query = new BasicDBObject().append("center",
				new BasicDBObject().append("$exists", true));
		DBCursor wikiCursor = wikiCollection.find(query);
		while (wikiCursor.hasNext()) {
			DBObject wikiitem = wikiCursor.next();
			String content = wikiitem.get("content").toString();
			if (content.startsWith("#")) {
				System.out.println("Ignore: " + wikiitem);
				continue;
			}

			String name = jc.t2s(wikiitem.get("title").toString());
			if (wikiitem.containsField("center")) {
				Float latitude = 0f, longitude = 0f;

				if (wikiitem.get("center") instanceof BasicDBList) {
					BasicDBList dblist = (BasicDBList) wikiitem.get("center");
					latitude = Float.valueOf(dblist.get(0).toString());
					longitude = Float.valueOf(dblist.get(1).toString());
				} else if (wikiitem.get("center") instanceof BasicDBObject) {

					latitude = Float.valueOf(((BasicDBObject) wikiitem
							.get("center")).get("0").toString());
					longitude = Float.valueOf(((BasicDBObject) wikiitem
							.get("center")).get("1").toString());
				}
				Float[] latilongti = new Float[2];
				latilongti[0] = latitude;
				latilongti[1] = longitude;

				geoItem.put(name, latilongti);
				System.out.println(name + "   " + latilongti[0] + "  "
						+ latilongti[1]);
			}
		}

		return geoItem;
	}
	
	
	
	public static void removeRegion(String regionId,double[] regionBorder){
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db = mgc.getDB();
		DBCollection poiCollection = db.getCollection("POI");
		DBCursor poiCursor = poiCollection.find(new BasicDBObject().append("regionId", new ObjectId(regionId)));
		Map<String,String> pendingWikititle=new HashMap<String,String>();
		Set<String> noregion=new HashSet<String>();
		while (poiCursor.hasNext()){
			DBObject dbo = poiCursor.next();
			String id=dbo.get("_id").toString();
			if (!dbo.containsField("wikititle")&&dbo.get("category").toString().equals("wikipedia")) {
				String wikititle = dbo.get("name").toString();
				pendingWikititle.put(id, wikititle);
			} 
			
			List<Double> center=(List<Double>) dbo.get("center");
			double latitude=center.get(0);
			double longitude=center.get(1);
			if(!(latitude>regionBorder[2]&&latitude<regionBorder[0]&&longitude>regionBorder[1]&&longitude<regionBorder[3])){
				noregion.add(id);
			}
			
		}
		
		for(String id: pendingWikititle.keySet()){
			DBObject updateQuery = new BasicDBObject("_id",new ObjectId(id));
			BasicDBObject updateConetent = new BasicDBObject().append(
					"wikititle", pendingWikititle.get(id));
			BasicDBObject newDocument = new BasicDBObject().append("$set",
					updateConetent);
			poiCollection.update(updateQuery, newDocument);
		}
		
		for(String id: noregion){
			DBObject updateQuery = new BasicDBObject("_id",new ObjectId(id));
			BasicDBObject updateConetent = new BasicDBObject().append(
					"regionId", new ObjectId("50bea542c46988843900000e"));
			BasicDBObject newDocument = new BasicDBObject().append("$set",
					updateConetent);
			poiCollection.update(updateQuery, newDocument);
		}
		
		
		
		
	}
	
	
	public static void appendWiki() throws IOException{
		

		HashMap<String, Float[]> wikiGeo = getWikipediaGeo();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db = mgc.getDB();

		DBCollection poiCollection = db.getCollection("POI");
		HashMap<String, Integer> pendingItem = new HashMap<String, Integer>();
		DBCursor poiCursor = poiCollection.find();
		while (poiCursor.hasNext()) {
			DBObject dbo = poiCursor.next();
			String wikititle = null;
			if (dbo.containsField("wikititle")) {
				wikititle = dbo.get("wikititle").toString();
			} else {
				String category = dbo.get("category").toString();
				if (category.equals("wikipedia"))
					wikititle = dbo.get("name").toString();
			}
			
			if (wikititle!=null&&wikiGeo.containsKey(wikititle)) {
				if(!dbo.containsField("center"))
					pendingItem.put(wikititle, 1);
				else{
					pendingItem.put(wikititle, 0);
					System.out.println("existing poi with geo information "+ wikititle);
				}
			}
		}
		
		for(Map.Entry<String, Float[]> entry:wikiGeo.entrySet()){
			String wikititle=entry.getKey();
			if(pendingItem.containsKey(wikititle)){
				if(pendingItem.get(wikititle)==1){
					System.out.println("Begin to add geo information for :"
							+ wikititle);
					DBObject updateQuery = new BasicDBObject("wikititle", wikititle);
					BasicDBObject updateConetent = new BasicDBObject().append(
							"center", entry.getValue());
					BasicDBObject newDocument = new BasicDBObject().append("$set",
							updateConetent);
					poiCollection.update(updateQuery, newDocument);
				}
					
			}else{
				System.out.println("Adding new poi " + wikititle);
				DBObject dbo = new BasicDBObject().append("name", wikititle)
						.append("category", "wikipedia")
						.append("wikititle", wikititle)
						.append("center", wikiGeo.get(wikititle));
				poiCollection.insert(dbo);
			}
			
		}
		
		
	}
	
	
	public static void updateWikiRegion(HashMap<String,double[]> regionBorderMap){
		
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db = mgc.getDB();
		DBCollection poiCollection = db.getCollection("POI");
		HashMap<String, Integer> pendingItem = new HashMap<String, Integer>();
		DBObject query=new BasicDBObject().append("category", "wikipedia");
		DBCursor poiCursor = poiCollection.find(query);
		Map<String,String> pendingWikititle=new HashMap<String,String>();
		Map<String,String> pendingRegion=new HashMap<String,String>();
		while (poiCursor.hasNext()) {
			DBObject dbo = poiCursor.next();
			String wikititle = dbo.get("name").toString();
			String id=dbo.get("_id").toString();
			if (!dbo.containsField("wikititle")) {
				pendingWikititle.put(id, wikititle);
			} 
			List<Double> center=(List<Double>) dbo.get("center");
			double latitude=center.get(0);
			double longitude=center.get(1);
			
			for(Map.Entry<String,double[]> entry:regionBorderMap.entrySet()){
				String region=entry.getKey();
				double[] regionBorder=entry.getValue();
				
			if(latitude>regionBorder[2]&&latitude<regionBorder[0]&&longitude>regionBorder[1]&&longitude<regionBorder[3]){
				pendingRegion.put(id,region);
				
			}
		}
		}
		for(String id: pendingRegion.keySet()){
			DBObject updateQuery = new BasicDBObject("_id",new ObjectId(id));
			BasicDBObject updateConetent = new BasicDBObject().append(
					"regionId", new ObjectId(pendingRegion.get(id)));
			BasicDBObject newDocument = new BasicDBObject().append("$set",
					updateConetent);
			poiCollection.update(updateQuery, newDocument);
		}
		
	}
	

	public static void main(String args[]) throws IOException {
		
		 BufferedReader br=new BufferedReader(new FileReader("cityborder"));
		 String line=null;
		 HashMap<String,double[]> borderMap=new HashMap<String,double[]>();
		 while((line=br.readLine())!=null){
			 String seg[]=line.split(":");
			 String regionid=seg[2];
			 String[] borderStr=seg[1].split(",");
			 double[] border=new double[4];
			 for(int j=0;j<4;j++){
				 border[j]=Double.parseDouble(borderStr[j]);
			 }
			 borderMap.put(regionid, border);
		 }
		 
		 //updateWikiRegion(borderMap);
		 removeRegion("4e8c091fd0c2ff482300031d",borderMap.get("4e8c091fd0c2ff482300031d"));
		
	}

}
