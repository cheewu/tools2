package com.zijiyou.text.dict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;

import com.chenlb.mmseg4j.Dictionary;
import com.chenlb.mmseg4j.analysis.ComplexAnalyzer;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.zijiyou.common.MapUtil;
import com.zijiyou.mongo.MongoConnector;

public class KeywordTFGenerator {

	/**
	 * @param args
	 */
	private static Set<String> wordSet =new TreeSet<String>();
	
	public static void main(String[] args) {
		
		if(args.length<3){
			System.out.println("Usage: KeywordTFGenerator <DictPath>  <WordCandidatePath> <OutPutFile>");
			System.exit(0);
		}
		String dictPath=args[0];
		String candidateFile=args[1];
		String outputFile=args[2];
		
		
		Analyzer analyzer =new ComplexAnalyzer(Dictionary.getInstance
				(dictPath));
		Map<String,Integer> occuredMap=new HashMap<String,Integer>();
		
		MongoConnector mgc=new MongoConnector(
				"analyzer.properties", "mongo_article");

		DB db=mgc.db;
		
		int i=0;
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader
					   (candidateFile));
			String line=null;
			
			while((line=in.readLine())!=null){
				if (line.trim().length()<2||wordSet.contains(line)){
					continue;
				}
				wordSet.add(line.trim());
				i++;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("词条总数:"+ i);
		
		DBCollection articleColl = db.getCollection("Article");
		DBCursor articleCur = articleColl.find();
		int documentCount=articleCur.count();
		System.out.println("article总数: "+documentCount);
		
		int j=0;
		while (articleCur.hasNext()) {
			if(++j%10000==0){
				System.out.println("Documents "+j+" Finished!");
			}
			
			DBObject document = articleCur.next();
			Set<String> documentWordSet=tokenizeParagraph(analyzer, document.get("content").toString());
			Iterator<String> wordIter=documentWordSet.iterator();
			while(wordIter.hasNext()){
				String word=wordIter.next();
				if(occuredMap.containsKey(word)&&occuredMap.get(word)!=null){
					Integer occur=occuredMap.get(word)+1;
					occuredMap.remove(word);
					occuredMap.put(word,occur);
				}else
					occuredMap.put(word, new Integer(1));
			}
		}
		
		Map<String,Integer> sorted_map=MapUtil.sortByValue(occuredMap);
        
        try {
			FileWriter fw=new FileWriter(outputFile);
			for (Map.Entry<String, Integer> entry : sorted_map.entrySet()) {
				fw.write(entry.getKey() + ","+entry.getValue()+","+KeywrodQuery.getKeywordCategoryName(entry.getKey())+"\n");
				fw.flush();
			}
			fw.close();
			 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
	}
	
	
	
	
	private static Set<String> tokenizeParagraph( Analyzer analyzer,String content) {
		
		// 一个段落中分词后得到的Term以及出现次数
		Set<String> kwSet = new HashSet<String>();

		// 对一个段落的文本内容进行分词处理
		Reader reader = new StringReader(content.trim());
		TokenStream ts = analyzer.tokenStream("", reader);
		ts.addAttribute(CharTermAttribute.class);
		
		try {
			while (ts.incrementToken()) {
				CharTermAttributeImpl attr = (CharTermAttributeImpl) ts.getAttribute(CharTermAttribute.class);
				String word = attr.toString().trim();
				
				if(word.length()<2)
					continue;
				
				if ( wordSet.contains(word)) 
					kwSet.add(word);
				}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return kwSet;
	}
}

 
