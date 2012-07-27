package com.zijiyou.text.fragment;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.common.MD5;
import com.zijiyou.mongo.MongoConnector;

public class FragmentPhotoExtractor {
	
	public static void main (String args[]) throws IOException{

		System.out.println(MD5.getMD5("http://s4.sinaimg.cn/bmiddle/5c65561et9475f83bcd43&690&690".getBytes()));
		
		Set<String> imageSet=dumpImages();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_article");
		DBCollection kwColl = mgc.db.getCollection("keywordsParagraph");
		BasicDBObject query = new BasicDBObject().append("documentID", "4ef1708cf776485e97006772");
		DBCursor dbs = kwColl.find();
		String realSrcPattern="\\sreal_src\\s*\\=\\\"([^\"]+)\\\"\\s";
		String srcPattern="\\ssrc\\s*\\=\\\"([^\"]+)\\\"\\s";
		Pattern realsrcp=Pattern.compile(realSrcPattern);
		Pattern srcp=Pattern.compile(srcPattern);
		FileWriter fw=new FileWriter("notfoundImage.txt");
		DBCollection imageCollection = mgc.db.getCollection("Images");
		
		while (dbs.hasNext()) {
			BasicDBObject imageDBO=new BasicDBObject();
			DBObject dbo = dbs.next();
			String referrer=dbo.get("url").toString();
			if(!dbo.containsField("pictures"))
				continue;
			List<String> pictures = (List<String>)dbo.get("pictures");
			for(String imageline:pictures){
				Matcher realm=realsrcp.matcher(imageline);
				String imageurl=null;
				if(realm.find()){
					imageurl=realm.group(1);
				}else{
					Matcher m=srcp.matcher(imageline);
					if(m.find())
						imageurl=m.group(1);
				}
				if(imageurl==null){
					fw.write(imageline+"\n");
					fw.flush();
					continue;
				}
				String host="";
				
				try{
					host=new URL(imageurl).getHost();
				}catch (Exception e){
					System.out.println("Wrong format URL,keywordParagraphID:"+dbo.get("_id").toString()+" URL:"+imageurl);
				}
				String md5=MD5.getMD5(imageurl.getBytes());
				if(imageSet.contains(md5)){
					System.out.println("Duplicate image:"+dbo.get("_id").toString() );
					continue;
				}
				imageSet.add(md5);
				imageDBO.append("domain",host).append("md5",md5).
					append("refer", referrer).append("status", 999).append("url",imageurl );
				imageCollection.insert(imageDBO);
				
			}
		}
		fw.close();
		mgc.close();
	}
	
	
	public static Set<String> dumpImages(){
		Set<String> imageMD5Set=new HashSet<String>();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_article");
		DBCollection kwColl = mgc.db.getCollection("Images");
		DBCursor dbs = kwColl.find();
		while(dbs.hasNext()){
			DBObject dbo=dbs.next();
			String md5=dbo.get("md5").toString();
			if(imageMD5Set.contains(md5)){
				System.out.println("Duplicate MD5:"+md5);
				kwColl.remove(dbo);
			}else
				imageMD5Set.add(md5);
		}
		mgc.close();
		return imageMD5Set;
	}
	
}
