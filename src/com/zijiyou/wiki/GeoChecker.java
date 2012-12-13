package com.zijiyou.wiki;

import java.io.FileWriter;
import java.io.IOException;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.mongo.MongoConnector;

public class GeoChecker {

	public static void main(String args[]) throws IOException {
		FileWriter geofw=new FileWriter("geo.txt");
		
		MongoConnector mgc=new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db =mgc.getDB();
		DBCollection poiCollection = db.getCollection("POI");
		DBObject query = new BasicDBObject().append("regionId", new ObjectId(
				"4e8c091fd0c2ff482300031d"));
		DBCursor poiCursor = poiCollection.find(query);
		while (poiCursor.hasNext()) {
			DBObject dbo = poiCursor.next();
			String id = dbo.get("_id").toString();
			if (dbo.containsField("center")) {
				if(dbo.get("center") instanceof Boolean)
					continue;
				BasicDBList dblist=(BasicDBList) dbo.get("center");
				float latitude=Float.valueOf(dblist.get(0).toString());
				float longitude=Float.valueOf(dblist.get(1).toString());
				
				
				float[] center=new float[2];
				center[0]=latitude;
				center[1]=longitude;
				
				if(center[1]>113.817 &&center[1]<114.408 &&center[0]>22.162&&center[0]<22.515){
					continue;
				}
				if(center[0]>113.817 &&center[0]<114.408 &&center[1]>22.163&&center[1]<22.515){
					float[] center2=new float[2];
					center2[0]=longitude;
					center2[1]=latitude;
					
					DBObject updateQuery = new BasicDBObject("_id", new ObjectId(id));
					BasicDBObject updateConetent = new BasicDBObject().append(
							"center",center2 );
					BasicDBObject newDocument = new BasicDBObject().append("$set",
							updateConetent);
					poiCollection.update(updateQuery, newDocument);
					}else{
						System.out.println("need to check"+ dbo.toString());
						
					}
				
				}
			} 

		poiCursor.close();
	}

}
