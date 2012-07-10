package com.zijiyou.text.fragment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;

//import org.htmlparser.*;
//import org.htmlparser.filters.TagNameFilter;
//import org.htmlparser.util.NodeList;
//import org.htmlparser.visitors.TextExtractingVisitor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.zijiyou.common.MapUtil;
import com.zijiyou.text.dict.DictGenerator;
import com.zijiyou.text.dict.KeywrodQuery;
import com.chenlb.mmseg4j.Dictionary;
import com.chenlb.mmseg4j.analysis.ComplexAnalyzer;

public class DocumentAnalyzer {

	private static final Logger LOG = Logger
			.getLogger(Fragmentizer.class);
	/** 分词器 */
	private static Analyzer analyzer;
	/** 自定义关键字词典：通常是根据需要进行定向整理的 */
	private static Set<String> wordSet = new HashSet<String>();
	/** 图片标签正则 */
	private static Pattern picturePattern = Pattern
			.compile("<\\s*img.+?/\\s*>");
	// #<\s*img.*?src\s*=\s*[\"']([^\"]*)[\"'].*?/\s*>#
	// (<img[^>]+>)

	/** 最短paragraph内容长度 **/
	private static int MIN_PARAGRAPH_LENGTH = 2;

	/** 用于分词处理的词典文件目录位置 */
	private static String dictPath;
	/** 用于抽取段落的景点关键词文件路径 */
	private static String wordsPath;

	/** 文档的objectid */
	private String documentObjectID;
	/** 文档的内容 */
	private String documentContent;
	/** 文档URL */
	private String url;
	/** 文档的Host boost */
	private float hostBoost = 1.0f;
	/**文档title**/
	private String title="";
	/**文档spiderName*/
	private String spiderName="";
	/**文档发表时间**/
	private String publishTime="";

	
	/** LinkedHashMap<文章段落序号, 文章段落内容> */
	private LinkedHashMap<Integer, String> pagraphContentMap = new LinkedHashMap<Integer, String>();
	/** LinkedHashMap<文章段落号，长度> **/
	private LinkedHashMap<Integer, Integer> pagraphLenMap = new LinkedHashMap<Integer, Integer>();
	/** HashMap<文章段落号，对应的图片列表> */
	private HashMap<Integer, List<String>> paragraphImageMap = new HashMap<Integer, List<String>>();

	/** 记录 一个关键字 在一个文档中出现的段落的分布 以及 关键对文章的得分 */
	private HashMap<String, Fragment> fragMap = new HashMap<String, Fragment>();
	/** 一篇文章包含的region列表 以及每个region的得分 */
	private Map<String, Double> regionScoreMap = new HashMap<String, Double>();
	/** 一个段落包含的关键词的列表 用于以后计算关联规则 */
	private List<BasicDBObject> wordsCocurrenceList = new ArrayList<BasicDBObject>();
	/** 文档最终的region */
	private String documentRegion = null;
	/** 文档的关键字列表 */
	private Map<String, Integer> docKwMap = new HashMap<String, Integer>();

	public static void initialize(Properties prop) {

		dictPath = prop.getProperty("dictPath");
		wordsPath = prop.getProperty("wordsPath");

		if (analyzer == null) {
			analyzer = new ComplexAnalyzer(Dictionary.getInstance(dictPath));
		}
		if (wordSet.size() < 1) {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						wordsPath));
				String line = null;
				while ((line = reader.readLine()) != null) {
					if (!line.isEmpty()) {
						wordSet.add(line.trim());
					}
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		
		if (LOG.isInfoEnabled())
			LOG.info("Analyzer initialized ");
	}

	
	public DocumentAnalyzer(String documentid, String content, String url,String title,String spiderName,String publishdate,
			float hostBoost, Properties prop) {
		this.documentContent = content
				.replaceAll("(\r?\n(\\s*\r?\n)+)", "\r\n");
		this.hostBoost = hostBoost;
		this.documentObjectID = documentid;
		this.url = url;
		this.title=title;
		this.spiderName=spiderName;
		this.publishTime=publishdate;
		initialize(prop);
	}

	public void analyze() {
		Long timestamp = System.currentTimeMillis();
		if (LOG.isInfoEnabled())
			LOG.info("Document " + this.documentObjectID
					+ "begin to analyze...");
		this.analyzeFragmentList();
		this.analyzeRegion();
		if (LOG.isInfoEnabled())
			LOG.info("Document " + this.documentObjectID
					+ "analyze finished, time consumed:"
					+ (System.currentTimeMillis() - timestamp));
	}

	/**
	 * 分析文章的一个段落
	 * 
	 * @return 关键词以及对应的数目
	 * @param content
	 *            段落内容
	 */
	private Map<String, Integer> tokenizeParagraph(String content) {
		// 一个段落中分词后得到的Term以及出现次数
		Map<String, Integer> keywordMap = new HashMap<String, Integer>();

		// 对一个段落的文本内容进行分词处理
		Reader reader = new StringReader(content.trim());
		TokenStream ts = analyzer.tokenStream("", reader);
		ts.addAttribute(CharTermAttribute.class);
		try {
			while (ts.incrementToken()) {

				CharTermAttributeImpl attr = (CharTermAttributeImpl) ts
						.getAttribute(CharTermAttribute.class);
				String word = attr.toString().trim();

				if (word.length() < 2)
					continue;

				if (wordSet.contains(word)) {
					// 统计文档中出现的所有的关键字列表
					if (docKwMap.containsKey(word)) {
						Integer count = docKwMap.get(word);
						docKwMap.remove(word);
						docKwMap.put(word, ++count);
					} else {
						docKwMap.put(word, 1);
					}

					if (keywordMap.containsKey(word)) {
						Integer count = keywordMap.get(word);
						keywordMap.remove(word);
						keywordMap.put(word, ++count);
					} else {
						keywordMap.put(word, 1);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return keywordMap;
	}

	/**
	 * 分析整个文章
	 * 
	 * @return 文章包含的关键词 以及每个关键词对应的fragment
	 */
	private void analyzeFragmentList() {

		String[] paragraphs = this.documentContent.split("\n+");
		int paragraphLength = paragraphs.length;
		int validPathLength = 0;

		/**
		 * Step1: a. 拆分段落 b. 计算段落长度 每个段落包含的图片列表 c. 对每个段落分词
		 */

		/** 每一个段落对应的 分词得到的term列表 */
		Map<Integer, Map<String, Integer>> paragraphTermList = new HashMap<Integer, Map<String, Integer>>();
		for (int i = 0; i < paragraphs.length; i++) {
			if (!paragraphs[i].isEmpty()) {
				pagraphContentMap.put(i, paragraphs[i].trim());

				List<String> oneParagraphImage = getPictures(pagraphContentMap
						.get(i));
				paragraphImageMap.put(i, oneParagraphImage);

				// 过滤掉段落中包含的图片 然后再去分词
				Matcher matcher = picturePattern.matcher(pagraphContentMap
						.get(i));
				String cleanContent = matcher.replaceAll("");
				int len = cleanContent.length();

				if (LOG.isDebugEnabled()) {
					LOG.debug("DocumentID " + this.documentObjectID + "  " + i
							+ "      " + len + "     " + paragraphs[i].trim());
				}

				// 如果段落长度太短则忽略
				if (len < MIN_PARAGRAPH_LENGTH)
					continue;
				pagraphLenMap.put(new Integer(i), len);
				validPathLength++;
				paragraphTermList.put(new Integer(i),
						tokenizeParagraph(cleanContent));
			}
		}

		/**
		 * Step2:每个关键字在文章中出现的段落id，以及对应id的出现次数
		 */
		HashMap<String, HashMap<Integer, Integer>> wordOccurence = new HashMap<String, HashMap<Integer, Integer>>();
		/**
		 * 段落id-->term,term出现次数
		 */
		Iterator<Entry<Integer, Map<String, Integer>>> pargraphIter = paragraphTermList
				.entrySet().iterator();
		while (pargraphIter.hasNext()) {
			Map.Entry<Integer, Map<String, Integer>> paragraphEntry = pargraphIter
					.next();
			Integer paragraphID = paragraphEntry.getKey();
			Map<String, Integer> paragraphTerms = paragraphEntry.getValue();
			Iterator<Entry<String, Integer>> wordIter = paragraphTerms
					.entrySet().iterator();

			Vector<String> keywordsParagraph = new Vector<String>();
			while (wordIter.hasNext()) {
				Map.Entry<String, Integer> entry = wordIter.next();
				String word = entry.getKey();
				keywordsParagraph.add(word);
				Integer freq = entry.getValue();
				if (LOG.isDebugEnabled())
					LOG.debug(paragraphID + " :  " + word + "   " + freq);
				if (wordOccurence.containsKey(word))
					wordOccurence.get(word).put(paragraphID, freq);
				else {
					HashMap<Integer, Integer> occurMap = new HashMap<Integer, Integer>();
					occurMap.put(paragraphID, freq);
					wordOccurence.put(word, occurMap);
				}
			}

			/**
			 * Step3: wordCocurrentObject :一个段落中出现的关键词列表以及 文章id 段落序号 用于挖掘关系
			 */
			if (keywordsParagraph.size() > 1) {
				BasicDBObject wordCocurrentObject = new BasicDBObject();
				wordCocurrentObject.append("keywords", keywordsParagraph);
				wordCocurrentObject.append("documentID", this.documentObjectID);
				wordCocurrentObject.append("paragraphID", paragraphID);
				wordsCocurrenceList.add(wordCocurrentObject);
				if (LOG.isDebugEnabled())
					LOG.debug(wordCocurrentObject);
			}
		}

		/**
		 * Step4: 找出一篇文章中所有的连续段落 并且以fragment的方式存储
		 */

		/**
		 * iterOccurence: 关键词--> <段落id->出现次数>
		 */
		Iterator<Entry<String, HashMap<Integer, Integer>>> iterOccurence = wordOccurence
				.entrySet().iterator();

		while (iterOccurence.hasNext()) {
			Map.Entry<String, HashMap<Integer, Integer>> entry = iterOccurence
					.next();
			String word = entry.getKey();
			HashMap<Integer, Integer> occurencList = entry.getValue();
			Fragment frag = new Fragment(word, documentObjectID, this.url,
					this.hostBoost);

			int occuredPath = 0;
			int start = -1;
			int end = -1;
			int subWordCount = 0;

			for (int j = 0; j < paragraphLength; j++) {
				Integer occurence = occurencList.get(j);
				if ((occurence == null || occurence == 0) && start > -1) {
					end = j - 1;
					frag.addSegment(start, end, subWordCount);
					subWordCount = 0;
					start = -1;
					end = -1;
				}

				if (occurence != null && occurence > 0) {
					occuredPath++;
					if (start == -1) {
						start = j;
					}
					subWordCount += occurence;

					if (j == paragraphLength - 1) {
						end = j;
						frag.addSegment(start, end, subWordCount);
						subWordCount = 0;
					}
				}
			}

			if (validPathLength > 0)
				frag.coverage = 100 * occuredPath / validPathLength;
			frag.addImages(this.paragraphImageMap);
			fragMap.put(word, frag);
		}
	}

	private void analyzeRegion() {

		HashMap<String, Vector<Double>> regionpoiMap = new HashMap<String, Vector<Double>>();
		Iterator<String> fragmentMapkeywordIter = fragMap.keySet().iterator();
		while (fragmentMapkeywordIter.hasNext()) {
			String kw = fragmentMapkeywordIter.next();

			Fragment frag = fragMap.get(kw);
			String keyword = frag.word;
			double score = frag.score;
			Integer kwinfo = KeywrodQuery.getKeywordCategory(keyword);

			if (kwinfo == null || kwinfo == 0) {
				LOG.error("DocumentAnalyzer关键词没有找到所属的类别： " + keyword);
				continue;
			}

			if (kwinfo < DictGenerator.CAT_REGION_MAX && score > 1) {
				regionScoreMap.put(keyword, score);
			}

			// 获得关键词的 parent 
			String parent = KeywrodQuery.getParent(kw);
			if (parent == null)
				continue;
			if (regionpoiMap.containsKey(parent)) {
				regionpoiMap.get(parent).add(score);
			} else {
				Vector<Double> plist = new Vector<Double>();
				plist.add(score);
				regionpoiMap.put(parent, plist);
			}
			
		}
		
		// 下面一段 基于目的地POI出现的次数 优化region得分
		Iterator<Entry<String, Vector<Double>>> poiIter = regionpoiMap
				.entrySet().iterator();
		while (poiIter.hasNext()) {
			Map.Entry<String, Vector<Double>> poiEntry = poiIter.next();
			String regionName = poiEntry.getKey();
			Vector<Double> vInt = poiEntry.getValue();
			/**
			 * 如果同一目的地出现的景点次数小于两个 则忽略景点对目的地的贡献
			 * */
			if (vInt.size() < 2)
				continue;

			double poiFactor = Math.sqrt(vInt.size());

			if (regionScoreMap.containsKey(regionName)) {
				double score = (regionScoreMap.get(regionName) * poiFactor);
				regionScoreMap.put(regionName, score);
				fragMap.get(regionName).score = score;
			}
		}

		// 确定文档的最终Region
		Map<String, Double> sorted_map = MapUtil.sortByValue(regionScoreMap);
		
		Set<String> todelset=new HashSet<String>();
		int j=0;
		for(String region:sorted_map.keySet()){
			if(++j>5)
				todelset.add(region);
		}

		for(String region:todelset){
			sorted_map.remove(region);
		}
		
		this.regionScoreMap = sorted_map;
		
		Iterator<String> regionsIter = sorted_map.keySet().iterator();
		if (regionsIter.hasNext())
			documentRegion = regionsIter.next();

		// 更新fragmentList
		for (Fragment value : fragMap.values()) {
			if (documentRegion != null)
				value.regionName = documentRegion;
		}

	}

	public HashMap<String, Fragment> getFragmentList() {
		return this.fragMap;
	}

	public BasicDBObject getDocumentObject() {

		BasicDBObject dbo = new BasicDBObject();
		dbo.append("documentID", this.documentObjectID);
		
		if (this.regionScoreMap != null)
			dbo.append("regions", this.regionScoreMap);
		
		
		dbo.append("paragraphs", this.pagraphContentMap);
		Map<String, Integer> sortedMap = MapUtil.sortByValue(docKwMap);
		int count = 0;
		Set<String> kwset = new HashSet<String>();
		for (Map.Entry<String, Integer> kw : sortedMap.entrySet()) {
			//keyword忽略region
			if(KeywrodQuery.getKeywordCategory(kw.getKey())==null){
					LOG.error("Keyword Category can't find for:"+ kw.getKey());
			}else
			if(KeywrodQuery.getKeywordCategory(kw.getKey())< DictGenerator.CAT_REGION_MAX)
					continue;
			
			if (++count > 10)
				break;
			kwset.add(kw.getKey());
		}
		
		dbo.append("url", url);
		dbo.append("spiderName",spiderName);
		dbo.append("title",title);
		dbo.append("publishTime", publishTime);
		dbo.append("keywords", kwset);
		
		return dbo;
	}

	public List<BasicDBObject> getWordOccurenceLists() {
		return this.wordsCocurrenceList;
	}

	private List<String> getPictures(String string) {
		Matcher matcher = picturePattern.matcher(string);
		List<String> pictures = new ArrayList<String>();
		if (!matcher.find())
			return null;
		for (int i = 0; i <= matcher.groupCount(); i++) {
			pictures.add(matcher.group(i));
		}
		;
		return pictures;
	}

}
