package com.zijiyou.text.fragment;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.zijiyou.mongo.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Article analysis tool.
 * 
 * @author shirdrn
 * @date 2011-12-13
 */
public class Fragmentizer {

	private static final Logger LOG = Logger.getLogger(Fragmentizer.class);

	public static void main(String[] args) {

		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_article");
		DB db = mgc.getDB();

		Properties hostProps = new Properties();
		try {
			FileInputStream fis = new FileInputStream("hostboost.properties");
			hostProps.load(fis);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Properties props = new Properties();
		try {
			FileInputStream fis = new FileInputStream("analyzer.properties");
			hostProps.load(fis);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/**
		 * 获得article列表
		 */
		DBCollection articleColl = db.getCollection("Article");
		String idString = "4ef17ff0f776485e9700ca5f";
		DBObject articleQuery = new BasicDBObject("_id", new ObjectId(idString));

		DBCursor articleCur = articleColl.find();
		DBCollection documentParagraphs = db.getCollection("articleParagraphs");
		DBCollection wordCocurrences = db.getCollection("wordsCooccurence");
		DBCollection keywordFragments = db.getCollection("keywordsParagraph");
		LOG.info("All matched articles count:" + articleColl.count());
		int i = 0;
		long begin = System.currentTimeMillis();
		while (articleCur.hasNext()) {
			i++;
			if (i % 1000 == 0)
				System.out.println(i + " documents finished,time used"
						+ (System.currentTimeMillis() - begin) / 1000 + "s");

			DBObject document = articleCur.next();
			String spiderName = document.get("spiderName") == null ? ""
					: document.get("spiderName").toString();
			float hostBoost = 1.0f;
			if (hostProps.containsKey(spiderName))
				hostBoost = Float.valueOf(hostProps.get(spiderName).toString()
						.trim());
			else
				LOG.error("Can't find boost value for spiderName:" + spiderName);

			// String title,String spiderName,String publishdate,

			String title = document.get("title") == null ? "" : document.get(
					"title").toString();
			String publishDate = document.get("publishDate") == null ? ""
					: document.get("publishDate").toString();

			DocumentAnalyzer doca = new DocumentAnalyzer(document.get("_id")
					.toString(), document.get("content").toString(), document
					.get("url").toString(), title, spiderName, publishDate,
					hostBoost, props);

			doca.analyze();

			HashMap<String, Fragment> fraglist = doca.getFragmentList();
			Iterator<String> iterstring = fraglist.keySet().iterator();
			while (iterstring.hasNext()) {
				BasicDBObject dbo = fraglist.get(iterstring.next())
						.toDBObject();
				if (LOG.isDebugEnabled()) {
					LOG.debug("Fragment:" + dbo);
				}
				keywordFragments.insert(dbo);
			}

			BasicDBObject dboDocument = doca.getDocumentObject();
			if (LOG.isDebugEnabled()) {
				LOG.debug("Documents:" + dboDocument);
			}
			documentParagraphs.insert(dboDocument);

			List<BasicDBObject> wordOccurrences = doca.getWordOccurenceLists();

			for (int j = 0; j < wordOccurrences.size(); j++) {
				wordCocurrences.insert(wordOccurrences.get(j));
				if (LOG.isDebugEnabled()) {
					LOG.debug("Word Occurences:" + wordOccurrences.get(j));
				}
			}

		}

		mgc.close();

	}
}
