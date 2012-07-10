package com.zijiyou.text.fragment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.BasicDBObject;

/**
 * 文章的一个片段：由一个或多个@Segment组成
 * 
 * @author shirdrn
 * @date 2011-12-15
 */
public class Fragment {
	/**关键词 */
	String word;
	/** 文章id*/
	String articleID;
	/** 文档的url**/
	String url;
	/** 文章的region**/
	public String regionName=null;
	
	/**包含的Segment列表*/
	List<Segment> segmentList=new ArrayList<Segment>(); 
	
	/**关键词在文章中出现的总次数**/
	private int wordCount;
	
	/** spidername boost*/
	float documentBoost=1.0f;

	/** Fragment中图片出现的次数**/
	private int pictureCount;
	
	/** Fragment中出现的图片列表 **/
	private List<String> pictureList=new ArrayList<String>();
	
	/**最后的得分**/
	double score=0f;
	
	/**关键词出现段落在文章中出现的比例**/
	public int coverage;
	
	public Fragment() {
		super();
	}
	
	
   public Fragment(String word, String articleid,String url,float docboost){
	  this.word=word;
	  this.articleID=articleid;
	  this.url=url;
	  this.documentBoost=docboost;
   }
	
   public void addSegment(Segment oneSegment){
	 this.wordCount+=oneSegment.wordCount;
	 this.score=Math.log1p(this.wordCount)*this.documentBoost;

	 if(this.segmentList.size()==0){
	 		this.segmentList.add(oneSegment);
	 		return;
	 }
	 
	 //如果当前的segment和之前的segment合并以后 有效的段落大于70% 则直接merge
	 Segment lastOne=this.segmentList.get(segmentList.size()-1);
	 int tomergeValidPath=lastOne.validParagraph+oneSegment.validParagraph;
	 int allParagraph=oneSegment.endParagraph-lastOne.startParagraph+1;
	 if (oneSegment.startParagraph-lastOne.endParagraph<4||allParagraph*0.7<tomergeValidPath){
		 lastOne.mergeWithOther(oneSegment);
	 }else
		 this.segmentList.add(oneSegment);
	 
   }
   
   public void addImages(HashMap<Integer,List<String>>  imageMap){
	   for (Segment seg :segmentList)
		  for(int j=seg.startParagraph ;j<seg.endParagraph+1;j++){
			  if(imageMap.get(j)!=null){
				  pictureList.addAll(imageMap.get(j));
				  this.pictureCount+=imageMap.get(j).size();
			  }
		  }
	   this.score=this.score*Math.sqrt(Math.log1p(1+this.pictureCount));
	   
   }
   
   
   
	public void addSegment (int startid,int endid,int wordCount){
		Segment newOne=new Segment(startid,endid,wordCount);
		this.addSegment(newOne);
	}
	
	
	public BasicDBObject toDBObject(){
		BasicDBObject dbobject=new BasicDBObject();
		dbobject.append("keyword",this.word );
		dbobject.append("score", this.score);
		if(this.regionName!=null)
			dbobject.append("region", this.regionName);
		if(this.pictureList.size()>0)
			dbobject.append("pictures",this.pictureList);
		dbobject.append("documentID", this.articleID);
		dbobject.append("url", this.url);
		List<BasicDBObject> paragraphs=new ArrayList<BasicDBObject>();
		for(int j=0;j<segmentList.size();j++){
			paragraphs.add(segmentList.get(j).toDBobject());
		}
		
		dbobject.append("paragraphs",paragraphs);
		dbobject.append("coverage",this.coverage);
		return dbobject;
		
	}
	
	@Override
	public String toString() {
		String segments="";
		for(int j=0;j<segmentList.size();j++){
			segments+=(segmentList.get(j).toDBobject());
		}
		
		return "[word=" + word + "articleid="+ articleID+ " paragraphIdList=" + 
						segments +"score: "+wordCount+"coverage"+coverage+"]";
	}
	
}

