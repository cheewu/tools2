package com.zijiyou.text.correlation;

import java.util.HashSet;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.mongo.MongoConnector;

public class RankUpdater {

	private static MongoConnector mgc=null;
	
	public static String getPoiID(String keyword,String regionID)  {
		
		DBCollection keywordColl = mgc.getDB().getCollection("keywordMap");
		BasicDBObject kwQuery = new BasicDBObject().append("name",keyword);
		DBCursor regionCur = keywordColl.find(kwQuery);
		String poiid=null;
		while (regionCur.hasNext()) {
			DBObject dbo = regionCur.next();
			if(dbo.get("poiID")!=null &&((DBObject)dbo.get("poiID")).containsField(regionID))
				poiid=((DBObject)dbo.get("poiID")).get(regionID).toString();
		}
		return poiid;
	}
	
	
	public static void main(String args[]){
		mgc=new MongoConnector("analyzer.properties", "mongo_tripfm");
		String regionID="4e8c0928d0c2ff4823000aab";
		DBCollection correlationColl = mgc.getDB().getCollection("correlation");
		DBCollection poiColl=mgc.getDB().getCollection("POI");
		BasicDBObject query = new BasicDBObject().append("name","巴黎");
		DBCursor cursor = correlationColl.find(query);
		Set<String> poiSet=new HashSet<String>();
		while(cursor.hasNext()){
			DBObject dbo=cursor.next();
			DBObject poiList=(DBObject)((DBObject)dbo.get("correlation")).get("attraction");
			for(String kw:poiList.keySet()){
				float index=Float.valueOf(poiList.get(kw).toString());
				System.out.print(kw);
				String poiid=getPoiID(kw,regionID);
				if(poiid==null){
					System.out.println(" no poiid");
					continue;
				}
				if(poiSet.contains(poiid)){
					System.out.println(kw+"  "+ poiid+" exists");
					continue;
				}	
				poiSet.add(poiid);
				System.out.println("   "+poiid);
				BasicDBObject updateQuery = new BasicDBObject().append("$set", 
						new BasicDBObject().append("newRank",index));	
				poiColl.update(new BasicDBObject().append("_id", new ObjectId(poiid)), updateQuery, false, true);
				
			}
		}
		
	}
	
	
	
	
}