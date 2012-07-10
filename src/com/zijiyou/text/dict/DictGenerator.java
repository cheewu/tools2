package com.zijiyou.text.dict;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.zijiyou.mongo.MongoConnector;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DictGenerator {

	private static final Logger LOG = Logger.getLogger(DictGenerator.class);

	private MongoConnector mgc = null;

	public static final int CAT_COUNTRY = 1001;
	public final static int CAT_PROVINCE = 1002;
	public final static int CAT_DESTINATION = 1003;
	public final static int CAT_REGION_OTHER = 1999;
	public final static int CAT_REGION_MAX=2000;
	
	public static final int CAT_AIRPORT = 2001;
	public static final int CAT_ATTRACTION = 2002;
	public static final int CAT_SUBATTRACTION = 2003;
	public static final int CAT_TRAIN = 2004;
	public static final int CAT_SHOPPING = 2005;
	public static final int CAT_SUBWAY = 2006;
	public static final int CAT_POI_OTHER = 2999;
	public static final int CAT_POI_MAX=3000;

	public static final int CAT_FOOD = 3001;
	public static final int CAT_ITEM = 3002;
	public static final int CAT_NOTE = 3003;
	public static final int CAT_PEOPLE = 3004;
	public static final int CAT_HISTORY = 3005;
	public static final int CAT_PRODUCT = 3006;
	public static final int CAT_TRANSPORTATION = 3007;
	public static final int CAT_ORGANIZATION=3008;
	public static final int CAT_KEYWORD_MAX=4000;

	public final static int CAT_OTHER = 9999;
	
	private  Map<String,String> regionIDMap=new HashMap<String,String>();
	private  Map<String,String> parentMap=new HashMap<String,String>();

	private DictGenerator(MongoConnector vmgc) {
		this.mgc = vmgc;
	}

	private Map<String, BasicDBObject> initRegion() {
		Map<String, BasicDBObject> regionkwMap = new HashMap<String, BasicDBObject>();

		DB db = mgc.db;
		DBCollection regionColl = db.getCollection("Region");
		BasicDBObject regionQuery = new BasicDBObject();
		String regioncategories[] = new String[] { "country", "province",
				"destination" };
		regionQuery.put("category", new BasicDBObject("$in", regioncategories));
		DBCursor regionCur = regionColl.find(regionQuery);

		while (regionCur.hasNext()) {
			DBObject regionObject = regionCur.next();
			if (!regionObject.containsField("name")) {
				System.out.println(regionObject);
				continue;
			}

			String regionName = regionObject.get("name").toString().trim();
			String category = regionObject.get("category").toString().trim();
			String regionID = regionObject.get("_id").toString().trim();
			String regionArea=null;
			if(regionObject.get("area")!=null)
				regionArea=regionObject.get("area").toString();
			int regionCat = DictGenerator.CAT_REGION_OTHER;
			if (category.equals("country"))
				regionCat = CAT_COUNTRY;

			if (category.equals("province"))
				regionCat = CAT_PROVINCE;

			if (category.equals("destination"))
				regionCat = CAT_DESTINATION;

			BasicDBObject regionDoc = new BasicDBObject()
					.append("name", regionName).append("category", regionCat)
					.append("regionID", regionID);

			if (regionkwMap.containsKey(regionName))
				LOG.error("Duplicat Region name: " + regionName + "  new"
						+ regionID + " oldid:"
						+ regionkwMap.get(regionName).getString("regionID"));
			else{
				regionkwMap.put(regionName, regionDoc);
				regionIDMap.put(regionID, regionName);
				if(regionArea!=null)
					parentMap.put(regionName,regionArea );
			}
		}

		regionCur.close();

		return regionkwMap;

	}

	private Map<String, BasicDBObject> initPOI() throws IOException {
		// 初始化POI
		Map<String, BasicDBObject> poikwMap = new HashMap<String, BasicDBObject>();

		DBCollection poiColl = mgc.db.getCollection("POI");
		BasicDBObject query = new BasicDBObject();
		query.put("category", "airport");
		DBCursor poiCur = poiColl.find();
		while (poiCur.hasNext()) {
			DBObject poiObject = poiCur.next();
			if (poiObject.get("name") == null) {
				poiColl.remove(poiObject);
				continue;
			}
			String pname = poiObject.get("name").toString().trim();
			String pid = poiObject.get("_id").toString().trim();

			String regionID = poiObject.get("regionId") == null ? ""
					: poiObject.get("regionId").toString().trim();
			
			String category = poiObject.get("category") == null ? "other"
					: poiObject.get("category").toString().trim();
			int poiCat = DictGenerator.CAT_POI_OTHER;

			if (category.equals("aiport"))
				poiCat = CAT_AIRPORT;

			if (category.equals("attraction"))
				poiCat = CAT_ATTRACTION;

			if (category.equals("subattraction"))
				poiCat = CAT_SUBATTRACTION;

			if (category.equals("train"))
				poiCat = CAT_TRAIN;

			if (category.equals("shopping"))
				poiCat = CAT_SHOPPING;

			if (category.equals("subway"))
				poiCat = CAT_SUBWAY;

			Map<String, String> poiRegionMap = new HashMap<String, String>();
			poiRegionMap.put(regionID, pid);

			String kw = poiObject.get("keyword") == null ? null : poiObject
					.get("keyword").toString();
			String[] keys = null;
			
			if (kw != null && !kw.equals(""))
				keys = poiObject.get("keyword").toString().split(",");
			else {
				keys = new String[1];
				keys[0] = pname;
			}
			
			for (int j = 0; j < keys.length; j++) {
				String poiKey=keys[j].trim();
				
				if(poiKey.length()<2)
					continue;
				
				if (poikwMap.containsKey(poiKey)) {
					BasicDBObject poidoc = poikwMap.get(poiKey);
					Map<String, String> existingPoi = (Map<String, String>) poidoc
							.get("poiID");
					existingPoi.putAll(poiRegionMap);
					int poiCount = poidoc.getInt("count") + 1;
					BasicDBObject poiDoc = new BasicDBObject()
							.append("name", poiKey).append("category", poiCat)
							.append("count", poiCount)
							.append("poiID", existingPoi);
					poikwMap.put(poiKey, poiDoc);

				} else {
					BasicDBObject poiDoc = new BasicDBObject()
							.append("name", poiKey).append("category", poiCat)
							.append("count", 1).append("poiID", poiRegionMap);
					poikwMap.put(poiKey, poiDoc);

				}
			}
		}

		for(Map.Entry<String,BasicDBObject> entry: poikwMap.entrySet()){
			String kw=entry.getKey();
			BasicDBObject dbo=entry.getValue();
			if (dbo.getInt("count")>1)
				continue;
			Map<String,String> poimap=(Map<String, String>) dbo.get("poiID");
			String regionID=poimap.keySet().iterator().next();
			if(regionIDMap.containsKey(regionID)){
				parentMap.put(kw, regionIDMap.get(regionID));
			}
		}
		poiCur.close();
		return poikwMap;
	}

	private Map<String, BasicDBObject> initKeyword() {

		Map<String, BasicDBObject> kwmap = new HashMap<String, BasicDBObject>();

		DBCollection kwColl = mgc.db.getCollection("Keyword");
		BasicDBObject query = new BasicDBObject();
		String categories[] = new String[] { "food", "note", "item" ,"organization","other","people","product"};
		query.put("category", new BasicDBObject("$in", categories));
		query.put("is_del", false);
		DBCursor kwCur = kwColl.find(query);
		Set<String> wikiSet = DictFileUtil.getDict("wiki_tf.txt","   ");

		while (kwCur.hasNext()) {
			DBObject poiObject = kwCur.next();
			String category = poiObject.get("category").toString();
			int poiCat = CAT_OTHER;
			if (category.equals("food"))
				poiCat = CAT_FOOD;

			if (category.equals("item"))
				poiCat = CAT_ITEM;

			if (category.equals("note"))
				poiCat = CAT_NOTE;

			if (category.equals("people"))
				poiCat = CAT_PEOPLE;

			if (category.equals("history"))
				poiCat = CAT_HISTORY;

			if (category.equals("product"))
				poiCat = CAT_PRODUCT;
			
			if (category.equals("organization"))
				poiCat = CAT_ORGANIZATION;
			
			
			if (category.equals("transportation"))
				poiCat = CAT_TRANSPORTATION;

			String kw = poiObject.get("keyword").toString();
			BasicDBObject kwDoc = new BasicDBObject().append("name", kw)
					.append("category", poiCat);
			if (wikiSet.contains(kw)) {
				kwDoc.append("wiki", kw);
			}else{
				System.out.println(kw);
			}
			kwmap.put(kw, kwDoc);

		}
		return kwmap;
	}

	public static boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}
	
	
	private void initRegionPOIKeyword() throws IOException{
		
		DBCollection kwColl = mgc.db.getCollection("keywordMap");

		Map<String, BasicDBObject> docmap = this.initRegion();
		System.out.println("region size:"+ docmap.size());
		for (Map.Entry<String, BasicDBObject> entry : docmap.entrySet()) {
			kwColl.insert(entry.getValue());

		}

		docmap = this.initPOI();
		for (Map.Entry<String, BasicDBObject> entry : docmap.entrySet()) {
			kwColl.insert(entry.getValue());
		}

		docmap = this.initKeyword();
		for (Map.Entry<String, BasicDBObject> entry : docmap.entrySet()) {
			kwColl.insert(entry.getValue());
		}
		
	}
	
	private  void initKeywordParentMap() throws IOException{
//		this.initRegion();
//		this.initPOI();
		DBCollection kwparentcoll=this.mgc.db.getCollection("keyParentMap");
		for (Map.Entry<String, String> entry: this.parentMap.entrySet()){
			kwparentcoll.insert(new BasicDBObject().append("key", entry.getKey()).append("parent",entry.getValue()));
		}
		
	}
	

	public static void main(String args[]) throws IOException {
		DictGenerator dicg = new DictGenerator(new MongoConnector(
				"analyzer.properties", "mongo_tripfm"));
		
		dicg.initRegionPOIKeyword();
		dicg.initKeywordParentMap();
	
		dicg.mgc.close();
		
	}

}
