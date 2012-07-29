package com.zijiyou.text.correlation;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zijiyou.common.MapUtil;
import com.zijiyou.mongo.MongoConnector;
import com.zijiyou.text.dict.DictGenerator;
import com.zijiyou.text.dict.KeywrodQuery;
public class Correlation {
	

	private static final Logger LOG = Logger.getLogger(Correlation.class);
	private static HashMap<Integer,List<Integer>> keywordInvertIndex=new HashMap<Integer, List<Integer>>();
	private static HashMap<Integer,List<Integer>> paragraphKeyword=new HashMap<Integer,List<Integer>>();
	private static Map<Integer,Integer>  keywordCountMap=null;
	
	private static HashMap<String,Integer> keywordMapID =new HashMap<String,Integer>();
  	private static HashMap<Integer,String> IDMapKeyword=new HashMap<Integer,String>();
	
	private static DB db=null;
	private static Integer kwIDSequence=0;
	
	private static String getStringbyID(Integer id){
		return IDMapKeyword.get(id);
	}
	
	
	private static Integer getIDbyString(String kw){
		if(keywordMapID.containsKey(kw))
			return keywordMapID.get(kw);
		kwIDSequence++;
		keywordMapID.put(kw, kwIDSequence);
		IDMapKeyword.put(kwIDSequence, kw);
		return kwIDSequence;
	}
	
	
	public static void buildIndex() {
		DBCollection coocurrenceColl = db.getCollection("wordsCooccurence");
		DBCursor mongocur = coocurrenceColl.find();
		Map<Integer,Integer> unsortedKeywordCountMap=new HashMap<Integer,Integer>();
		int j=0;
		while(mongocur.hasNext()){
			j++;
			if(j>5000000)
				break;
			if(j%10000==0)
				System.out.println(j+"  finished...");
			
			DBObject dbo=mongocur.next();
			String document=dbo.get("_id").toString();
			Integer docid=getIDbyString(document);
			List<String> kws=(List<String>) dbo.get("keywords");
			List<Integer> kwids=new ArrayList<Integer>();
			
			for(String kw: kws){
				Integer kwid=getIDbyString(kw);
				kwids.add(kwid);
				if(unsortedKeywordCountMap.containsKey(kwid)){
					Integer count=unsortedKeywordCountMap.get(kwid)+1;
					unsortedKeywordCountMap.put(kwid, count);
				}else
					unsortedKeywordCountMap.put(kwid, new Integer(1));
				
				if(keywordInvertIndex.containsKey(kwid))
					keywordInvertIndex.get(kwid).add(docid);
				else
				{   List<Integer> list=new ArrayList<Integer>();
					list.add(docid);
					keywordInvertIndex.put(kwid,list );
					
				}
			}
			paragraphKeyword.put(docid, kwids);

		}
		System.out.println("Mongo articicle finished,Begin to sort keywordCount map....");
		
		keywordCountMap=MapUtil.sortByValue(unsortedKeywordCountMap);
		
		System.out.println("Map sort finished,begin to write file keyworcount ");
		
		try {
			FileWriter fw = new FileWriter("KeywordCount.txt");
			for (Map.Entry<Integer, Integer> entry : keywordCountMap.entrySet()) {
				fw.write(getStringbyID(entry.getKey()) + "   " + entry.getValue() + "\n");
				fw.flush();
			}
			fw.close();
			System.out.println("keyword count finished,begin to write keyword invert index。。。");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Build index finished...");
	}
	
	public static void analyzerRelation(int minOccur,int minRatio,boolean insert) throws IOException{
		DBCollection correlationColl = db.getCollection("correlation");
		FileWriter fw=new FileWriter("correlations.txt");
		int j=0;
		for (Map.Entry<Integer, Integer> m : keywordCountMap.entrySet()) {
			
			if(m.getValue()<100)
				continue;
			Integer kw=m.getKey();
			Integer kwcategory=KeywrodQuery.getKeywordCategory(getStringbyID(kw));
			if(kwcategory==null){
				LOG.error("Can't find category for keyword:"+kw);
				continue;
			}
			if (kwcategory<DictGenerator.CAT_REGION_MAX){
				
				System.out.println("Begin to get relation for: "+ getStringbyID(m.getKey())+"  "+m.getValue());
				HashMap<Integer,Integer>  candidateMap=new HashMap<Integer,Integer>();
				List<Integer>  kwParagraphList= keywordInvertIndex.get(kw);

				for(int paragraph:kwParagraphList){
					List<Integer> paragraphWords=paragraphKeyword.get(paragraph);
					for(Integer kwcandidate :paragraphWords){
						if(kwcandidate.equals(kw))
							continue;
						
						if(candidateMap.containsKey(kwcandidate)){
							Integer count=candidateMap.get(kwcandidate)+1;
							candidateMap.put(kwcandidate,count);
						}else
							candidateMap.put(kwcandidate, new Integer(1));
					}
				}
				
				// 计算candidatemap中的关键词在包含kw的关键词的文档中出现的比例 这个比例要比关键词在整个文档中出现的比例高MINRATIO倍;
				Map<Integer,Double> candidateScoreMap=new HashMap<Integer,Double>();
				
				for(Map.Entry<Integer, Integer> entry: candidateMap.entrySet()){
					Integer kwcandidate=entry.getKey();
					Integer kwcandidateCount=entry.getValue();
					if (kwcandidateCount<minOccur){
						continue;
					}
					
					double baseratio= (double)keywordCountMap.get(kwcandidate)/(500);
					double joinratio=((double)10000*kwcandidateCount/(double)keywordCountMap.get(kw));
					
					NumberFormat  ddf1=NumberFormat.getNumberInstance();
					ddf1.setMaximumFractionDigits(2);
					
                    if(joinratio>minRatio*baseratio){
						fw.write(getStringbyID(kw)+", "+getStringbyID(kwcandidate)+" ,"+kwcandidateCount+" ,"+ddf1.format(baseratio)+" ,"+
                                ddf1.format(joinratio)+" ,"+KeywrodQuery.getKeywordCategory(getStringbyID(kwcandidate))+"\n");
						fw.flush();
						double score= Math.sqrt(kwcandidateCount)*Math.log(joinratio/baseratio);
						candidateScoreMap.put(kwcandidate, score);
					}
				}
				
				
				Map<Integer,Double> sortedCandidateMap=MapUtil.sortByValue(candidateScoreMap);
				
				// 把一个关键词所有的关系按照类别归类 country,province,destination,poi,food,item,note 每个类别对应一个hashmap
				Map<String,Map<String,Double>> categoryCorrelationMap=new HashMap<String,Map<String,Double>>();
				
				
				for (Map.Entry<Integer, Double> entry: sortedCandidateMap.entrySet()){
					
					String category=KeywrodQuery.getKeywordCategoryName(getStringbyID(entry.getKey()));
					if (categoryCorrelationMap.containsKey(category)){
						categoryCorrelationMap.get(category).put(getStringbyID(entry.getKey()), entry.getValue());
					}else{
						Map<String,Double> oneCategoryMap=new HashMap<String,Double>();
						oneCategoryMap.put(getStringbyID(entry.getKey()), entry.getValue());
						categoryCorrelationMap.put(category, MapUtil.sortByValue(oneCategoryMap));
						
					}
					
				}
				
				if(categoryCorrelationMap.size()==0)
					continue;
				if(insert){
					BasicDBObject dbo=new BasicDBObject();
					dbo.append("name",getStringbyID(kw));
					dbo.append("correlation",categoryCorrelationMap);
					correlationColl.insert(dbo);
				
				}
			}
			
		}
		
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		
		if(args.length<3){
			System.out.println("Usage Correlation <MinOccur> <MinRatio> <isInsert>");
			System.exit(0);
		}
		Integer occur=Integer.parseInt(args[0]);
		Integer ratio=Integer.parseInt(args[1]);
		boolean insert=false;
		if (args[2].equals("true")){
			insert=true;
		}
		
		db=MongoConnector.getDBByProperties("analyzer.properties","mongo_article");
		buildIndex();
		try {
			analyzerRelation(occur,ratio,insert);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}










