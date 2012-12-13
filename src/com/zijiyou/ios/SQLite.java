package com.zijiyou.ios;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.mongo.MongoConnector;

public class SQLite {

	private static Connection conn = null;
	private static MongoConnector mgc = null;
	private static String regionID = "4e8c091fd0c2ff482300031d";
	private static String dbfile = "poi_hk.db";
	private static String wikititle="香港";
	private static  Map<String,Set<Integer>> stationLineMap=new HashMap<String,Set<Integer>>();
	private static Map<String,String> stationNameMap=new HashMap<String,String>();
	private static Map<Integer,String> lineidMap=new HashMap<Integer,String>();

	public static Connection getConnection(String sqliteFile) throws Exception {
		if (conn == null || conn.isClosed()) {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + sqliteFile);
		}
		return conn;
	}

	public static String getWikiContent(String wikititle) throws IOException {
		
		DBCollection wikiColl = mgc.getDB().getCollection("Wikipedia");
		BasicDBObject query = new BasicDBObject().append("title", wikititle);
		DBCursor dbs = wikiColl.find(query);
		String content = null;
		while (dbs.hasNext()) {
			DBObject dbo=dbs.next();
			content = dbo.get("content").toString();
			if (content != null && !content.toUpperCase().startsWith("#"))
				break;
		}

		if (content != null && content.toUpperCase().startsWith("#")) {
			System.out.println(wikititle + "  " + content);
			content = null;
		}

		Map<String, String> contentSegments = new LinkedHashMap<String, String>();
		String segmentKey = "General";

		if (content == null)
			return null;

		BufferedReader br = new BufferedReader(new StringReader(content));
		String segmentContent = "";
		String line = null;
		
		while ((line = br.readLine()) != null) {
			
			if (line.startsWith("=")&& !line.startsWith("====")) {
				contentSegments.put(segmentKey, segmentContent);
				segmentKey = line;
				segmentContent = "";
			} else {
				if (line.startsWith("===="))
					line.replace("====", "***");
					
				//if (line.trim().length() > 2)
					segmentContent += line + "\n";
			}
		}
		// 最后一个没结束的..
		contentSegments.put(segmentKey, segmentContent);

		//transform hashmap to json
		//BasicDBObject dbo = new BasicDBObject();
		Iterator<String> keyIter = contentSegments.keySet().iterator();
		String previousKey = null;
		String previousValue = null;
		List<BasicDBObject> dboList=new ArrayList<BasicDBObject>();
		List<HashMap<String,String>> l3Content = new ArrayList<HashMap<String,String>>();
		while (keyIter.hasNext()) {
			String key = keyIter.next();
			String value = contentSegments.get(key);

			if (key.equals("General"))
				dboList.add(new BasicDBObject().append(key, contentSegments.get(key)));
			if (key.startsWith("==")&&!key.startsWith("===")) {
				if (previousKey != null) {
					if (l3Content.isEmpty()) {
						if (previousValue != null)
							dboList.add(new BasicDBObject().append(previousKey, previousValue));
					} else {
							dboList.add(new BasicDBObject().append(previousKey, l3Content));
							l3Content=new ArrayList<HashMap<String,String>>();
						
					}
				}
				previousKey = key.replace("=", "").replace(" ", "");
				previousValue = value;
				if (previousValue.length() < 5)
					previousValue = null;
				
			}

			if (key.startsWith("===") && value.length() > 5) {
				if(l3Content.isEmpty()&&previousValue!=null){
					HashMap<String,String> l3tmpMap=new HashMap<String,String>();
					l3tmpMap.put("", previousValue);
					l3Content.add(l3tmpMap);
				}
				HashMap<String,String> l3tmpMap=new HashMap<String,String>();
				l3tmpMap.put(key.replace("=", "").replace(" ", ""), value);
				l3Content.add(l3tmpMap);
			}

			if (key.startsWith("===="))
				System.out.println("segment4 "+key+"   "+contentSegments.get(key));

		}

		return dboList.toString();

	}

	public static byte[] readImage(String f) throws IOException {

		File file = new File(f);
		FileInputStream fis = new FileInputStream(f);
		int len = (int) file.length();
		byte[] bytes = new byte[len];
		fis.read(bytes);
		return bytes;
	}

	public static void writeDescription() {

		try {
			Connection con = getConnection(dbfile);
			Statement stat = con.createStatement();
			String ddl = "create table if not exists citydescription(dindex integer primary key,dproperty varchar(10), dvalue text)";
			stat.execute(ddl);

			PreparedStatement insertStmt = con
					.prepareStatement("insert into citydescription(dproperty,dvalue) "
							+ "values(?,?)");

			
			insertStmt.setString(1, "");
			insertStmt.setString(2, getWikiContent(wikititle));
			insertStmt.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void writePOI() {
		
		try {
			Connection con = getConnection(dbfile);
			Statement stat = con.createStatement();
			// create poi table
			String ddl = "create table  if not exists poi("
					+ "poiid integer primary key," + "poimongoid  varchar(50),"
					+ "name varchar(500), " + "rank float," + "latitude flat,"
					+ "longitude float," + "address varchar(50) ,"
					+ "opentime varchar(50) ," + "telephone varchar(20),"
					+ "subway varchar(200)," + "description text,"
					+ "category varchar(20)," + "ticket varchar(30),"
					+ "image blob," + "line varchar(500)" + ")";
			System.out.println(ddl);
			stat.execute(ddl);

			// select mongodb
			DBCollection poiColl = mgc.getDB().getCollection("POI");
			BasicDBObject query = new BasicDBObject().append("regionId",
					new ObjectId(regionID));
			DBCursor dbs = poiColl.find(query);
			PreparedStatement insertStmt = con
					.prepareStatement("insert into poi (name,rank,latitude,longitude,address,"
							+ "opentime,telephone,subway,description,category,ticket,image,poimongoid,line) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

			while (dbs.hasNext()) {
				
				DBObject dbo = dbs.next();
				if (dbo.get("center") == null) {
					continue;
				}
				
				if (dbo.containsField("is_del")
						&& dbo.get("is_del").toString().equals("true"))
					continue;

				if (!dbo.containsField("category")) {
					System.out.println(dbo);
					continue;
				}

				String poimongoid = dbo.get("_id").toString();
				System.out.println(dbo.get("name") + "  " + dbo.get("_id"));

				Float latitude = 0f, longitude = 0f;
				if (dbo.get("center") instanceof BasicDBList) {
					BasicDBList dblist = (BasicDBList) dbo.get("center");
					latitude = Float.valueOf(dblist.get(0).toString());
					longitude = Float.valueOf(dblist.get(1).toString());
				} else if (dbo.get("center") instanceof BasicDBObject) {
					latitude = Float
							.valueOf(((BasicDBObject) dbo.get("center")).get(
									"0").toString());
					longitude = Float.valueOf(((BasicDBObject) dbo
							.get("center")).get("1").toString());
				}
				
				if(!(latitude>22.159&&latitude<22.533&&longitude>113.828&&longitude<114.406)){
					System.out.println("NOT IN HK  " +dbo.get("name").toString()+"  "+dbo.get("center").toString());
					continue;
				}

				// DES Encrypt poi name
				String name = DES.encrypt(dbo.get("name").toString());

				float rank = 0.0f;
				if (dbo.get("newRank") != null)
					rank = Float.parseFloat(dbo.get("newRank").toString());
				String address = "";

				if (dbo.get("address") != null) {
					address = dbo.get("address").toString();
				}

				String opentime = "";
				if (dbo.get("openTime") != null)
					opentime = dbo.get("openTime").toString();
				String telephone = "";
				if (dbo.get("telNum") != null)
					telephone = dbo.get("telNum").toString();
				
				List<BasicDBObject> stations = new ArrayList<BasicDBObject>();
				String subway="";
				//todo subway
				if (dbo.get("nearSubwayStation") != null) {
					BasicDBList stationList= (BasicDBList) dbo.get("nearSubwayStation");
					for(Object station: stationList){
						BasicDBObject stationdbo=(BasicDBObject)station;
						String  stationpid=stationdbo.get("poiId").toString();
						stations.add(new BasicDBObject().append("poimongoid",stationpid).append("lineid", stationLineMap.get(stationpid))
							.append("name", stationNameMap.get(stationpid)).append("dis",Float.parseFloat(stationdbo.getString("dis"))));
						
					}
					subway=stations.toString();
				
				}

				String desc = "";
				String wikititle = null;
				if (dbo.get("wikititle") != null) {
					wikititle = dbo.get("wikititle").toString();
				} else if (dbo.get("category").equals("wikipedia")) {
					wikititle = dbo.get("name").toString();
				}
				if (wikititle != null)
					desc = getWikiContent(wikititle);

				if (desc == null || desc.equals(""))
					if (dbo.get("desc") != null) {
						desc = dbo.get("desc").toString();
					}
				
				if (desc == null || desc.equals("")) {
					System.out.println(dbo.get("name") + "  " + dbo.get("_id")
							+ "  no desc");
					continue;
				}

				String category = "";
				if (dbo.get("category") != null)
					category = dbo.get("category").toString();
				
				String ticket = "";
				if (dbo.get("ticket") != null)
					ticket = dbo.get("ticket").toString();
				
				
				
				String line = "";
				BasicDBObject linedbo=new BasicDBObject();
				if (stationLineMap.get(poimongoid) != null) {
					Set<Integer> lines= stationLineMap.get(poimongoid);
					for(Integer linenumber:lines){
						linedbo.append(linenumber.toString(),new BasicDBObject().append("name", lineidMap.get(linenumber)).append("order", "0"));
					}
					line=linedbo.toString();
				}

				insertStmt.setString(1, name);
				insertStmt.setFloat(2, rank);
				insertStmt.setFloat(3, latitude);
				insertStmt.setFloat(4, longitude);
				insertStmt.setString(5, address);
				insertStmt.setString(6, opentime);
				insertStmt.setString(7, telephone);
				insertStmt.setString(8, subway);
				insertStmt.setString(9, desc);
				insertStmt.setString(10, category);
				insertStmt.setString(11, ticket);

				if (category != null && !category.equals("wikipedia")
						&& !category.equals("subway")) {
					// Get POI Images
					String imageId = null;
					String imageType = null;

					if ((dbo.get("googleImages") instanceof BasicDBList)) {

						BasicDBList imageList = (BasicDBList) dbo
								.get("googleImages");

						for (int j = 0; j < imageList.size(); j++) {
							BasicDBObject image = (BasicDBObject) imageList
									.get(j);
							if (image != null) {
								imageId = image.getString("imageId");
								imageType = image.getString("imageType");
								if (imageId != null && imageType != null)
									break;
							}
						}
					} else if (dbo.get("googleImages") instanceof BasicDBObject) {
						if (((BasicDBObject) dbo.get("googleImages"))
								.containsField("imageId")) {
							imageId = ((BasicDBObject) dbo.get("googleImages"))
									.getString("imageId");
							imageType = ((BasicDBObject) dbo
									.get("googleImages"))
									.getString("imageType");
						} else {

							imageId = ((BasicDBObject) ((BasicDBObject) dbo
									.get("googleImages")).get("0"))
									.getString("imageId");
							imageType = ((BasicDBObject) ((BasicDBObject) dbo
									.get("googleImages")).get("0"))
									.getString("imageType");
						}
					} else {
						System.out.println(dbo.get("name") + "  "
								+ dbo.get("_id") + "fail:" + dbo);
						continue;
					}

					if (imageId == null || imageType == null) {
						System.out.println(dbo.get("name") + "  "
								+ dbo.get("_id") + " no images");
						continue;
					}

					String imgURL = "http://regionpoipic.b0.upaiyun.com/POI/"
							+ imageId + "." + imageType + "!w588";
					String imageName = imageId + ".jpg";
					String imagePath = "poi";
					insertStmt.setBytes(12, TileFileCrawler.downloadImage(
							imgURL, imagePath, imageName));
				} else
					insertStmt.setObject(12, null);

				insertStmt.setString(14, line);
				insertStmt.setString(13, poimongoid);
				insertStmt.executeUpdate();
				//System.out.println("  OK");
			}
			// build index
			stat.execute("create  index idx_poi on poi(category,rank);");
			stat.execute("create index indx_lati on poi(latitude);");
			stat.execute("create index indx_longti on poi(longitude);");
			stat.execute("create index index_poiid on poi(poimongoid);");

			stat.close();
			con.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void writeSubway() {
		
		DBCollection poiCollection = mgc.getDB().getCollection("POI");
		DBObject qpoi = new BasicDBObject().append("category", "subway").append("regionId",new ObjectId(regionID));
		DBCursor pCursor = poiCollection.find(qpoi);

		while (pCursor.hasNext()) {
			DBObject dbo = pCursor.next();
			String poiid = dbo.get("_id").toString();
			String name = dbo.get("name").toString();
			Set<Integer> lineNumberSet=new HashSet<Integer>();
			stationNameMap.put(poiid, name);
			if(dbo.get("subway") instanceof com.mongodb.BasicDBList){
				BasicDBList lineList=(BasicDBList)dbo.get("subway");
				for(Object line:lineList){
					BasicDBObject linedbo=(BasicDBObject)line;
					lineNumberSet.add(Integer.parseInt(linedbo.getString("lineId")));
				}
				stationLineMap.put(poiid, lineNumberSet);
				System.out.println(poiid+"  "+ lineNumberSet);
			}else{
				System.out.println("Wrong format: "+name+"  "+dbo);
				
			}
		}
		
		try {
			Connection con = getConnection(dbfile);
			Statement stat = con.createStatement();
			// create poi table
			String ddl = "create table  if not exists subway("
					+ "lineid integer primary key,"
					+ "subwaysystem  varchar(50)," + "linename varchar(50), "
					+ "color char(7)," + "stationlist varchar(5000)" + ")";
			System.out.println(ddl);
			stat.execute(ddl);

			// select mongodb
			DBCollection subwayColl = mgc.getDB().getCollection("Subway");

			BasicDBObject query = new BasicDBObject().append("regionId",
					new ObjectId(regionID));
			DBCursor dbs = subwayColl.find(query);
			PreparedStatement insertStmt = con
					.prepareStatement("insert into subway "
							+ "(lineid,subwaysystem,linename,color,stationlist)"
							+ "values(?,?,?,?,?)");

			while (dbs.hasNext()) {
				DBObject dbo = dbs.next();
				System.out.println(dbo);
				int lineid = Integer.parseInt(dbo.get("lineId").toString());
				String subwaysystem = dbo.get("system").toString();
				String linename = dbo.get("name").toString();
				String color = dbo.get("color").toString();
				lineidMap.put(lineid, linename);

				List<BasicDBObject> subLineList = (List<BasicDBObject>) dbo
						.get("subline");
				List<BasicDBObject> newStationList = new ArrayList<BasicDBObject>();
				for (BasicDBObject bdbo : subLineList) {
					List<BasicDBObject> stationList=(List<BasicDBObject>)bdbo.get("list");
					for(BasicDBObject stationDbo:stationList){
						String stationOrder=stationDbo.getString("order");
						stationDbo.removeField("order");
						stationDbo.put("stationOrder", stationOrder);

						String stationPid = stationDbo.getString("poiId").toString();
						stationDbo.remove("poiId");
						stationDbo.put("poimongoid", DES.encrypt(stationPid));
						
						Set<Integer> lines= new HashSet<Integer>();
						
						
						for(Integer line:stationLineMap.get(stationPid)){
							if (line!=lineid)
								lines.add(line);
						}
						
						stationDbo.put("transferLine",lines);
						
						String stationMinute=stationDbo.getString("minute");
						stationDbo.removeField("minute");
						stationDbo.put("stationMinute", stationMinute);
						
						newStationList.add(stationDbo);
					}
				}

				insertStmt.setInt(1, lineid);
				insertStmt.setString(2, subwaysystem);
				insertStmt.setString(3, linename);
				insertStmt.setString(4, color);
				insertStmt.setString(5, newStationList.toString());
				insertStmt.executeUpdate();
			}
			con.close();

		} catch (Exception e) {
			
			
			e.printStackTrace();
		}
	}

	

	
	
	public static void main(String args[]) throws Exception {

		mgc = new MongoConnector("analyzer.properties", "mongo_tripfm");
		writeSubway();
		writePOI();
		writeDescription();
		mgc.close();

	}

}
