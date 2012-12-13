package com.zijiyou.text.dict;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.mongo.MongoConnector;
import com.zijiyou.text.correlation.Correlation;

public class KeywrodQuery {
	private static MongoConnector mgc = null;
	private static Map<String, Integer> keywordCategoryMap = new HashMap<String, Integer>();
	private static Map<String, String> keywordParentMap = new HashMap<String, String>();

	private static final Logger LOG = Logger.getLogger(KeywrodQuery.class);
	
	public static Integer getKeywordCategory(String kw) {
		if (keywordCategoryMap.size() == 0)
			keywordCategoryMap = dumpKeywordCategoryMap("keywordMap");

		return keywordCategoryMap.get(kw);
	}

	public static boolean isPOI(int keyword) {
		return (keyword > DictGenerator.CAT_REGION_MAX && keyword < DictGenerator.CAT_POI_MAX);

	}

	public static String getKeywordCategoryName(String kw) {
		Integer category = getKeywordCategory(kw);
		if (category == null) {
			return "other";
		}

		String categir = "other";
		switch (category) {
		case DictGenerator.CAT_AIRPORT: {
			categir = "airport";
			break;
		}
		case DictGenerator.CAT_ATTRACTION: {
			categir = "attraction";
			break;
		}
		case DictGenerator.CAT_COUNTRY: {
			categir = "country";
			break;
		}
		case DictGenerator.CAT_DESTINATION: {
			categir = "destination";
			break;
		}
		case DictGenerator.CAT_FOOD: {
			categir = "food";
			break;
		}
		case DictGenerator.CAT_HISTORY: {
			categir = "history";
			break;
		}
		case DictGenerator.CAT_ITEM: {
			categir = "item";
			break;
		}
		case DictGenerator.CAT_NOTE: {
			categir = "note";
			break;
		}
		case DictGenerator.CAT_ORGANIZATION: {
			categir = "organization";
			break;
		}
		case DictGenerator.CAT_OTHER: {
			categir = "other";
			break;
		}
		case DictGenerator.CAT_PEOPLE: {
			categir = "people";
			break;
		}
		case DictGenerator.CAT_POI_OTHER: {
			categir = "poi_other";
			break;
		}
		case DictGenerator.CAT_PRODUCT: {
			categir = "product";
			break;
		}
		case DictGenerator.CAT_PROVINCE: {
			categir = "province";
			break;
		}
		case DictGenerator.CAT_SHOPPING: {
			categir = "shopping";
			break;
		}
		case DictGenerator.CAT_SUBATTRACTION: {
			categir = "subattraction";
			break;
		}
		case DictGenerator.CAT_SUBWAY: {
			categir = "subway";
			break;
		}
		case DictGenerator.CAT_TRAIN: {
			categir = "train";
			break;
		}
		case DictGenerator.CAT_TRANSPORTATION:
			categir = "transportation";

		}
		return categir;

	}

	public static String getParent(String kw) {
		if (keywordParentMap.size() == 0)
			keywordParentMap = dumpKeywordParentMap();

		return keywordParentMap.get(kw);

	}

	private static Map<String, Integer> dumpKeywordCategoryMap(String collection) {
		
		LOG.info("Begin to dump keywordMap....");
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DBCollection kw = mgc.getDB().getCollection(collection);

		DBCursor kwCur = kw.find();
		Map<String, Integer> resultMap = new HashMap<String, Integer>();
		while (kwCur.hasNext()) {
			DBObject dbo = kwCur.next();
			String key = dbo.get("name").toString();
			if (key.length() < 2) {
				System.out.println(dbo);
				continue;
			}
			if (!resultMap.containsKey(key)) {

				String category = dbo.get("category").toString();
				if (category.matches("\\d*"))
					resultMap.put(key,
							Integer.parseInt(dbo.get("category").toString()));
				else{
					
					Integer catint=Integer.parseInt(category.substring(0, category.length()-2));
					LOG.error("Wrong category information..."+ dbo);
					resultMap.put(key, 9999);
					};
			}
		}

		mgc.close();
		LOG.info("KeywordDump finished ,dump size:"+ resultMap.size());
		
		return resultMap;

	}

	public static void writeTravelKeyword(String file) throws IOException {
		Map<String, Integer> keywordCategoryMap = dumpKeywordCategoryMap("keywordMap");
		FileWriter fw = new FileWriter(file);
		for (Map.Entry<String, Integer> entry : keywordCategoryMap.entrySet()) {
			fw.write(entry.getKey() + "\n");
			fw.flush();
		}
		fw.close();

	}

	private static Map<String, String> dumpKeywordParentMap() {
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DBCollection kw = mgc.getDB().getCollection("keyParentMap");
		DBCursor kwCur = kw.find();
		Map<String, String> resultMap = new HashMap<String, String>();

		while (kwCur.hasNext()) {
			DBObject dbo = kwCur.next();
			String key = dbo.get("key").toString();
			if (!resultMap.containsKey(key)) {
				resultMap.put(key, dbo.get("parent").toString());
			}
		}
		mgc.close();
		return resultMap;

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// Map<String,Integer> keywordMap=dumpKeywordCategoryMap("keywordMap2");

		Map<String, Integer> keywordMap2 = dumpKeywordCategoryMap("keywordMap");

		// for(String kw: keywordMap.keySet()){
		// if(!keywordMap2.containsKey(kw))
		// System.out.println(kw);
		//
		// }

		// TODO Auto-generated method stub
		// writeTravelKeyword("wordstravel.dic");
		// System.out.println(getKeywordCategory("法院"));
	}

}
