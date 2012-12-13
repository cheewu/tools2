package com.zijiyou.text.fragment;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.common.MD5;
import com.zijiyou.mongo.MongoConnector;
import com.zijiyou.text.dict.DictGenerator;

public class FragmentPhotoExtractor {
	
	
	public static void  getHighPriorityPictures() throws IOException{
		
		MongoConnector mgc=new MongoConnector("analyzer.properties","mongo_tripfm");
		DBCollection kwColl = mgc.getDB().getCollection("keywordMap");
		BasicDBObject query = new BasicDBObject().append("idf", new BasicDBObject().append("$exists","true"));
		DBCursor dbs = kwColl.find(query).sort(new BasicDBObject("idf",1));

		// get image Map
		Map<String,Integer> imageMap=getImageMap("Images0729");
		
		// 1. Get high priority keywors 
		Queue<String> hiKWList=new  LinkedList<String>(); 
		while (dbs.hasNext()){
			DBObject dbo=dbs.next();
			//挑选出符合条件的kw 包括 country province destination 和attraction
			if(!dbo.containsField("category"))
				continue;
			else
			{   String categorystr=dbo.get("category").toString();
				if(!categorystr.matches("\\d+"))
					continue;
				int category=Integer.parseInt(dbo.get("category").toString());
				if(category>DictGenerator.CAT_DESTINATION&&category!=DictGenerator.CAT_ATTRACTION)
					continue;
			}
			hiKWList.add(dbo.get("name").toString());
		}
		
		System.out.println("Keyword candidate list: "+ hiKWList.size());
		
		mgc=new MongoConnector("analyzer.properties","mongo_article");
		
		//2. 获得imagelist 以及对应的status 然后导出csv文件
		FileWriter fw=new FileWriter("imagestatus.csv");
		
		DBCollection pageColl=mgc.getDB().getCollection("keywordsParagraph");
		DBCollection imageColl=mgc.getDB().getCollection("Images0729");
		for(String kw:hiKWList){
			
			query=new BasicDBObject().append("keyword", kw);
			dbs=pageColl.find(query).sort(new BasicDBObject().append("score", -1)).limit(30);
			//System.out.println(kw+ " document size:"+dbs.length());
			while(dbs.hasNext()){
				DBObject dbo = dbs.next();
				
				if(!dbo.containsField("pictures"))
					continue;
				String url=dbo.get("url").toString();
				String id=dbo.get("_id").toString();
				String documentid=dbo.get("documentID").toString();
				List<String> pictures = (List<String>)dbo.get("pictures");
				for(String imageline:pictures){
					String imageurl=getImageURL(imageline);
					if(imageurl!=null){
						String md5= MD5.getMD5(imageurl.getBytes());
						Integer status=imageMap.get(md5);
						
						if(status!=null){
							if(status==0){
								imageColl.update(new BasicDBObject().append("md5",md5 ), new BasicDBObject("$set",new BasicDBObject().append("status", "999")));
								fw.write(kw+","+status+","+md5+","+imageurl+","+url+"\n");
								fw.flush();
							}
						}else{
							String host="";
							try{
								host=new URL(imageurl).getHost();
							}catch (Exception e){
								System.out.println("Wrong format URL,keywordParagraphID:"+dbo.get("_id").toString()+" URL:"+imageurl);
							}
							DBObject imageDBO=new BasicDBObject().append("domain",host).append("md5",md5).append("documentid",documentid).
							append("refer", url).append("status", 999).append("url",imageurl );
							imageColl.insert(imageDBO);
							
							fw.write(kw+","+999+","+md5+","+imageurl+","+url+"\n");
							fw.flush();
							System.out.println("Can't find image object for image md5:"+ md5+" keywordsParagraphid"+id);
						}
						
					}
				}
			}
		}
		
	}
	
	public static String getImageURL(String imageline){
		String realSrcPattern="\\sreal_src\\s*\\=\\\"([^\"]+)\\\"\\s";
		String srcPattern="\\ssrc\\s*\\=\\\"([^\"]+)\\\"\\s";
		Pattern realsrcp=Pattern.compile(realSrcPattern);
		Pattern srcp=Pattern.compile(srcPattern);
		Matcher realm=realsrcp.matcher(imageline);
		String imageurl=null;
		if(realm.find()){
			imageurl=realm.group(1);
		}else{
			Matcher m=srcp.matcher(imageline);
			if(m.find())
				imageurl=m.group(1);
		}
		return imageurl;
	
	}
	
	
	public static void main (String args[]) throws IOException{
		if(args.length<1)
			System.out.println("Usage FragmentPhotoExtractor 1|2| \n  1: extract images  2:get high priority pictures");
		int select=Integer.parseInt(args[0]);
		switch(select){
		case 1: 
			extractFragmentImages();
		case 2:
			getHighPriorityPictures();
		}
		
	}
	
	
	public static void extractFragmentImages() throws IOException{
		
		Set<String> imageSet=dumpImages("Images");
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_article");
		DBCollection kwColl = mgc.getDB().getCollection("Article");
		
		DBCursor dbs = kwColl.find();
		String realSrcPattern="\\sreal_src\\s*\\=\\\"([^\"]+)\\\"\\s";
		String srcPattern="\\ssrc\\s*\\=\\\"([^\"]+)\\\"\\s";
		Pattern realsrcp=Pattern.compile(realSrcPattern);
		Pattern srcp=Pattern.compile(srcPattern);
		FileWriter fw=new FileWriter("notfoundImage.txt");
		DBCollection imageCollection = mgc.getDB().getCollection("keywordsParagraph");
		int count=0;
		int imageSumCount=0;
		while (dbs.hasNext()) {
			if(++count%10000==0)
				System.out.println(count+ " paragraphs has finished!");
			
			DBObject dbo = dbs.next();
			String paragraphID=dbo.get("_id").toString();
			String referrer=dbo.get("url").toString();
			if(!dbo.containsField("pictures"))
				continue;
			String documentID=dbo.get("_id").toString();
			List<String> pictures = (List<String>)dbo.get("pictures");
			int imageCount=0;
			for(String imageline:pictures){
				BasicDBObject imageDBO=new BasicDBObject();
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
				imageCount++;
				String md5=MD5.getMD5(imageurl.getBytes());
				if(imageSet.contains(md5)){
					continue;
				}
				imageSet.add(md5);
				imageDBO.append("domain",host).append("md5",md5).append("documentid",documentID).
					append("refer", referrer).append("status", 999).append("url",imageurl );
				imageCollection.insert(imageDBO);
				imageSumCount++;
				
			}
			System.out.println("Finished Paragraph :"+paragraphID +"  imageCount:"+imageCount);
		}
		System.out.println("all images get "+imageSumCount);
		fw.close();
		mgc.close();
		
	}
	
	
	public static void diffCollection(){
		Set<String> smallSet=dumpImages("Images");
		Set<String> bigSet=dumpImages("Images2_0729");
		for(String md5: bigSet){
			if(!smallSet.contains(md5)){
				System.out.println(md5);
			}
		}
		
	}
	
	
	private static Map<String,Integer>  getImageMap(String imageCollection){
		Map<String,Integer> imageMap=new HashMap<String,Integer>();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_article");
		DBCollection kwColl = mgc.getDB().getCollection(imageCollection);
		DBCursor dbs = kwColl.find();
		while(dbs.hasNext()){
			DBObject dbo=dbs.next();
			String md5=dbo.get("md5").toString();
			Integer status=Integer.parseInt(dbo.get("status").toString());
			imageMap.put(md5, status);
		}
		mgc.close();
		return imageMap;
	}
	
	
	private static Set<String> dumpImages(String imageCollection){
		Set<String> imageMD5Set=new HashSet<String>();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_article");
		DBCollection kwColl = mgc.getDB().getCollection(imageCollection);
		DBCursor dbs = kwColl.find();
		while(dbs.hasNext()){
			DBObject dbo=dbs.next();
			String md5=dbo.get("md5").toString();
		    imageMD5Set.add(md5);
		}
		mgc.close();
		return imageMD5Set;
	}
	
}
