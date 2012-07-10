package com.zijiyou.text.fragment;

import com.mongodb.BasicDBObject;

/**文章的一个segment 可以是若干个连续的段落 或者是若干连
 * 续段落的组合
 */
public class Segment{
	/**Segment 段落的起始位置**/
	int startParagraph=0;
	int endParagraph=0;
	
	/** Segment中单词出现的次数*/
	int wordCount;
	
	/**Segment中包含关键字的段落数 可能是两个Segment合并的*/
	int validParagraph=0;
	
	/**Segment中图片链接列表*/
	//<String> pictureList=new ArrayList<String>();
	
	public Segment(int start,int end,int wordCount){
		this.startParagraph=start;
		this.endParagraph=end;
		this.wordCount=wordCount;
		//this.pictureList=pictures;
		this.validParagraph=this.endParagraph-this.startParagraph+1;
	}
	
	public void mergeWithOther(Segment seg1){
		this.endParagraph=seg1.endParagraph;
		//this.pictureList.addAll(seg1.pictureList);
		this.wordCount+=seg1.wordCount;
		this.validParagraph+=seg1.validParagraph;
	}
	
	public BasicDBObject toDBobject(){
		BasicDBObject dbobject=new BasicDBObject();
		//dbobject.append("pictures",this.pictureList);
		dbobject.append("start", this.startParagraph);
		dbobject.append("end", this.endParagraph);
		dbobject.append("wordCount", this.wordCount);
		return dbobject;
	}
}



