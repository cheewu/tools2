package com.zijiyou.wiki;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;

import com.zijiyou.mongo.MongoConnector;
import com.zijiyou.wiki.DocObj;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import taobe.tec.jcc.JChineseConvertor;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class WikipediaZhExtractor {
	private static final Logger LOG = Logger.getLogger(WikipediaZhExtractor.class);
	protected DBCollection collection;
	JChineseConvertor jc;
	
	public WikipediaZhExtractor() throws IOException {
		jc=JChineseConvertor.getInstance();
		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");
		DB db = mgc.getDB();
		
		collection=db.getCollection("Wikipedia2");
	}
	
	private InputStream stringToStream(String s) {
		InputStream is = null;
		try {
			is = new ByteArrayInputStream(s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return is;
	}
	
	private void parse(InputStream in) {
		Document doc = null;
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			docBuilderFactory.setIgnoringComments(true);
			docBuilderFactory.setNamespaceAware(true);
			DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			doc = builder.parse(in);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Element root = doc.getDocumentElement();
		if(root.getTagName().equals("page")) {
			CoordDocObj obj = new CoordDocObj();
			NodeList pageNodes = root.getChildNodes();
			for (int j = 0; j < pageNodes.getLength(); j++) {
				Node revisionNode = pageNodes.item(j);
				if(revisionNode instanceof Element) {
					Element revisionElement = (Element) revisionNode;
					if(((Element)revisionNode).getTagName().equals("revision")) {
						NodeList textNodes = revisionElement.getChildNodes();
						parse(textNodes, obj);
					} else if(((Element)revisionNode).getTagName().equals("id")) {
						
						obj.setId(revisionElement.getTextContent());
					} else if(((Element)revisionNode).getTagName().equals("title")) {
						obj.setTitle(revisionElement.getTextContent());
					}
				}
			}
		}
	}
	
	private void parse(NodeList textNodes, CoordDocObj obj) {
		for (int i = 0; i < textNodes.getLength(); i++) {
			Node node = textNodes.item(i);
			if((node instanceof Element) && ((Element)node).getTagName().equals("text")) {
				Element textElement = (Element) node;
				String text = textElement.getTextContent();
				try {
					extractFromText(text, obj);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	Pattern bracePattern = Pattern.compile("\\{\\{[^\\}]*\\}\\}");
	
	/**
	 * 抽取感兴趣的内容
	 * @param text
	 * @param obj
	 * @throws IOException
	 */
	private void extractFromText(String text, CoordDocObj obj) throws IOException {
		extractCoord(text, obj);
		// extract type
		extractType(text, obj);
		// extract english name
		extractEnglishName(text, obj);
		// extract body content
		extractContent(text, obj);
		// insert to mongodb
		insert(obj);
	}
	
	
	// [[en:Forbidden City]]
	Pattern englishNamePattern = Pattern.compile("\\[\\[en:([^\\]]*)\\]\\]");
	private void extractEnglishName(String text, DocObj obj) {
		Matcher m = englishNamePattern.matcher(text);
		if(m.find()) {
			obj.setEnglishName(m.group(1));
		}
	}
	
	// {{coord|22|21|21.94|N|114|12|23.66|E|type:mountain}}
	// {{coord|25|3|45.52|N|121|11|52.32|E|type:city(80000)_region:TW-TAO|display=title}}
	// 排除：list-style-type: none;
	Pattern typePattern = Pattern.compile("type:([^\\|_:\\(]+).*\\}\\}");
	private void extractType(String text, DocObj obj) {
		Matcher m = typePattern.matcher(text);
		if(m.find()) {
			obj.setType(m.group(1));
		}
	}
	
	

	private void extractContent(String text, DocObj obj) {
		// 下面每段处理必须保证顺序
		
		text = text.replaceAll("\\&[a-zA-Z]{1,10};", "").replaceAll("<[^>]*>", ""); // 如：<div class="references-small">
		text = text.replaceAll("「", "\"").replaceAll("」", "\""); // 如：在賽場上他有個綽號叫「梅赫西迪先生」（Monsieur Mercédès）
		text = text.replaceAll("'''", "\""); // 如：'''晚清时期（1860年－1912年）'''
//		"&amp;", // 如：|[[象雄语]]||&amp;nbsp;||喜马拉雅语支
//		"&nbsp;", // 如：|[[象雄语]]||&amp;nbsp;||喜马拉雅语支
		text = text.replaceAll("&amp;", " ").replaceAll("nbsp;", " ");
		
		// 如：// '''物理學'''（{{lang-en|'''Physics'''}}）
		// 如：它经常被称为'''纽约市'''（'''{{lang|en|New York City}}'''，官方名稱為'''{{lang|en|The City of New York}}'''），因为它位于[[纽约州]]东南部。
		// 如：特別的慶典有[[格林尼治村]]的[[:en:New York's Village Halloween Parade|萬聖節遊行]]、[[:en:Tribeca Film Festival|Tribeca電影節]]和中央公園免費的夏日舞台。
		text = new Cleaner(text, new String[] {
				"(\\{\\{lang[^\\|]*[^\\}]+\\}\\})",
				"(\\[\\:[a-zA-Z^:]+:[^\\]]+\\]\\])",
		}) {
			
			@Override
			public void clean() {
				for(Pattern pattern : this.getPatterns()) {
					Matcher m = pattern.matcher(text);
					while(m.find()) {
						String origin = m.group(1);
						String s = origin.replace("{{", "").replace("}}", "");
						String[] a = s.split("\\|");
						text = text.replace(origin, a[a.length-1]);
					}
				}
			}
			
		}.run();
		
		// -{臺北}- 加粗体符号清除
		text = new Cleaner(text, new String[] {
				"(\\-\\{[^\\}]+\\}\\-)", 
		}) {

			@Override
			public void clean() {
				for(Pattern pattern : this.getPatterns()) {
					Matcher m = pattern.matcher(text);
					while(m.find()) {
						String origin = m.group(1);
						String s = origin.replace("-{", "").replace("}-", "");
						text = text.replace(origin, s);
					}
				}
			}
			
		}.run();
		
		text = text.replaceAll("\\{\\{", "{").replaceAll("\\}\\}", "}"); // 如：{{=>{, }}=>}
		
		// 2.
		text = new Cleaner(text, new String[]{
//				"(\\{\\{[^\\}]*\\{\\{[^\\}]*\\}\\}[^\\{]*\\}\\})", // 如：{{ .*{{ .* }} .* }}
				"(\\{[^\\{\\}]*\\})", // 双层嵌套：至此那个第一遍，如：{ .* }
				"(\\{[^\\{\\}]*\\})", // 双层嵌套：至此那个第二遍，如：{ .* }
				"(\\{[^\\{\\}]*\\})", // 双层嵌套：至此那个第三遍，如：{ .* }
				"(\\{[^\\{\\}]*\\})", // 双层嵌套：至此那个第四遍，如：{ .* }
				"(\\[\\[[^:\\]]+:[^\\]]+\\]\\]\\s*)", // 如：[[sv:Topografi]]
				"(\\[\\[File:[^\\]]+\\]\\])", // 清理图片[[File:HK Kennedy Town shop 60414.jpg|thumb|200px|竹製品是[[山貨]]之一]]
				}) {

			@Override
			public void clean() {
				for(Pattern pattern : this.getPatterns()) {
					Matcher m = pattern.matcher(text);
					while(m.find()) {
						String fragment = m.group(1);
						text = text.replace(fragment, "");
					}
				}
			}

		}.run();
		
//		System.out.println(text);
		
		// 3.
		text = new Cleaner(text, new String[] {
				"\\[\\[([^\\|]*\\|[^\\]]*)\\]\\]", // [[583年|开皇三年]]
				}) {

			@Override
			public void clean() {
				for(Pattern pattern : this.getPatterns()) {
					Matcher m = pattern.matcher(text);
					while(m.find()) {
						String origin = m.group();
						String fragment = m.group(1);
						String[] a = fragment.trim().split("\\|");
						if(a.length==2) {
							String newFragment = a[0] + "(" + a[1] + ")";
							text = text.replace(origin, newFragment);
						} else if(a.length==1) {
							String newFragment = a[0];
							text = text.replace(origin, newFragment);
						}
					}
				}
			}
			
		}.run();
		
		// 4.
		text = new Cleaner(text, new String[]{
				"(\\[\\[)", // 如：[[
				"(\\]\\])", // 如：]]
				"(\\[)", // 如：[
				"(\\])", // 如：]
				"([a-zA-z]+://[^\\s]*)", // url
				"(\\(\\s*\\))", // 如：( )
				"(（\\s*）)", // 如：（）中文括号 2011/11/03
				"(\\[\\s*\\])", // 如：[ ]
				"(\\{\\s*\\})", // 如：{ }
				}) {

			@Override
			public void clean() {
				text = text.trim();
				for(Pattern pattern : this.getPatterns()) {
					Matcher m = pattern.matcher(text);
					while(m.find()) {
						String fragment = m.group(1);
						text = text.trim().replace(fragment, "");
					}
				}
			}

		}.run();
		
		
		obj.setContent(text.trim());
	}
	
	abstract class Cleaner {
		protected Pattern[] patterns;
		protected String text;
		protected String textBackup;
		public Pattern[] getPatterns() {
			return patterns;
		}
		public Cleaner(String text, String[] patterns) {
			Pattern[] ps = new Pattern[patterns.length];
			for (int i = 0; i < ps.length; i++) {
				ps[i] = Pattern.compile(patterns[i]);
			}
			this.patterns = ps;
			this.text = text;
			this.textBackup = new String(text);
		}
		public String run() {
			boolean encounterException = false;
			try {
				clean();
			} catch (Exception e) {
				encounterException = true;
			}
			return encounterException ? textBackup : text;
		}
		
		public abstract void clean() throws Exception;
	}
	
	// 
	
//	// 度分：{{coord|22|34|N|114|08|E|region:CN_type:city|display=title}}
//	static Pattern coordPattern1 = Pattern.compile("\\{\\{[cC]oord\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([NnSs]{1})\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([EeWw]{1})\\|");
//	// 度分秒：{{coord|22|34|41|N|114|08|34|E|region:CN_type:city|display=title}}
//	static Pattern coordPattern2 = Pattern.compile("\\{\\{[cC]oord\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([NnSs]{1})\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([EeWw]{1})\\|");
//	
//	// NE（小数）：{{coord|30.18|N|106.18|E|display=title|scale_region:CN_type:county}}
//	static Pattern coordPattern10 = Pattern.compile("\\{\\{[cC]oord\\|([\\-]?\\d+[\\.]?[\\d]*)\\|([NnSs]{1})\\|([\\-]?\\d+[\\.]?[\\d]*)\\|([EeWw]{1})\\|");
//	// 度分（整数）：{{coord|30|N|106|E|display=title|scale_region:CN_type:county}}
//	static Pattern coordPattern11 = Pattern.compile("\\{\\{[cC]oord\\|([\\-]?\\d+)\\|([NnSs]{1})\\|([\\-]?\\d+)\\|([EeWw]{1})\\|");
//	// 小数：{{coord|53.469722|-2.298889|region:US_type:landmark|display=title}}
//	static Pattern coordPattern12 = Pattern.compile("\\{\\{[cC]oord\\|([\\-]?\\d+\\.\\d+)\\|([\\-]?\\d+\\.\\d+)\\|");
//	// 整数小数混合：{{coord|-24|56|38.26|N|121|12|57.83|E|type:city(205000)_region:TW-TAO|display=title}}
//	static Pattern coordPattern13 = Pattern.compile("\\{\\{[cC]oord\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([\\-]?\\d+\\.\\d+)\\|([NnSs]{1})\\|([\\-]?\\d+)\\|([\\-]?\\d+)\\|([\\-]?\\d+\\.\\d+)\\|([EeWw]{1})\\|");
	// {{coord|1=22|2=23|3=39.48|4=N|5=113|6=58|7=3|8=E|9=type:railwaystation}}
	// 待处理
	
	// 整数/小数度分秒：{{coord|22|34|41|N|114|08|34|E|region:CN_type:city|display=title}}
	// 整数/小数度分秒：{{coord|49.0722|34|41|N|114|08|34|E|region:CN_type:city|display=title}}
	// 整数/小数度分秒：{{coord|34|00||N|109|00||E|display=title}} 双竖线
	static Pattern coordPattern1 = Pattern.compile("(\\{\\{[cC]oord\\|(\\d+[\\.]?\\d*)\\|(\\d+[\\.]?\\d*)\\|(\\d+[\\.]?\\d*)[\\|]+([NnSs]{1})\\|(\\d+[\\.]?\\d*)\\|(\\d+[\\.]?\\d*)\\|(\\d+[\\.]?\\d*)[\\|]+([EeWw]{1})\\|)");
	// 整数/小数度分：{{coord|22|34|N|114|08|E|region:CN_type:city|display=title}}
	// 整数/小数度分：{{coord|22.88|34|N|114.98|08|E|region:CN_type:city|display=title}}
	static Pattern coordPattern2 = Pattern.compile("(\\{\\{[cC]oord\\|(\\d+[\\.]?\\d*)\\|(\\d+[\\.]?\\d*)[\\|]+([NnSs]{1})\\|(\\d+[\\.]?\\d*)\\|(\\d+[\\.]?\\d*)[\\|]+([EeWw]{1})\\|)");
	// 整数/小数度：{{coord|30.18|N|106.18|E|display=title|scale_region:CN_type:county}}
	// 整数/小数度：{{coord|30.18|N|106.18|E|display=title|scale_region:CN_type:county}}
	static Pattern coordPattern3 = Pattern.compile("(\\{\\{[cC]oord\\|(\\d+[\\.]?\\d*)[\\|]+([NnSs]{1})\\|(\\d+[\\.]?\\d*)[\\|]+([EeWw]{1})\\|)");
	// 小数：{{coord|53.469722|-2.298889|region:US_type:landmark|display=title}}
	// 整数：{{coord|53|-29|region:US_type:landmark|display=title}}
	static Pattern coordPattern4 = Pattern.compile("(\\{\\{[cC]oord\\|([\\-]?\\d+[\\.]?\\d*)\\|([\\-]?\\d+[\\.]?\\d*)\\|)");
	static LinkedHashMap<Pattern, Integer> type = new LinkedHashMap<Pattern, Integer>();
	static { // 注意：要保证顺序
		type.put(coordPattern1, 1);
		type.put(coordPattern2, 2);
		type.put(coordPattern3, 3);
		type.put(coordPattern4, 4);
	}
	private void extractCoord(String text, CoordDocObj obj) {
		Iterator<Entry<Pattern, Integer>> iter = type.entrySet().iterator();
		boolean got = false;
		while(iter.hasNext()) {
			Entry<Pattern, Integer> entry = iter.next();
			Matcher m = entry.getKey().matcher(text);
			switch(type.get(entry.getKey()).intValue()) {
				case 1 :
					if(m.find()) {
//						System.out.println("1->" + m.group(1));
						translateCoords(m.group(5), m.group(9), m.group(2), m.group(3), m.group(4), 
								m.group(6), m.group(7), m.group(8), obj);
						got = true;
						break;
					}
				case 2 :
					if(m.find()) {
//						System.out.println("2->" + m.group(1));
						translateCoords(m.group(4), m.group(7), m.group(2), m.group(3), "00", 
								m.group(5), m.group(6), "00", obj);
						got = true;
						break;
					}
				case 3 :
					if(m.find()) {
//						System.out.println("3->" + m.group(1));
						translateCoords(m.group(3), m.group(5), m.group(2), "00", "00", 
								m.group(4), "00", "00", obj);
						got = true;
						break;
					}
				case 4 :
					if(m.find()) {
//						System.out.println("4->" + m.group(1));
						obj.setLatitude(toDouble(m.group(2)));
						obj.setLongitude(toDouble(m.group(3)));
						got = true;
						break;
					}
			}
			if(got) {
				break;
			}
		}
	}
	
	class CoordDocObj extends DocObj {
		protected String ns;
		protected String ew;
		protected String nsLat;
		protected String ewLng;
		public CoordDocObj() {
			super();
		}
		public CoordDocObj(String ns, String ew, String nsLat, String ewLng) {
			super();
			this.ns = ns;
			this.ew = ew;
			this.nsLat = nsLat;
			this.ewLng = ewLng;
		}
		public String getNs() {
			return ns;
		}
		public void setNs(String ns) {
			this.ns = ns;
		}
		public String getEw() {
			return ew;
		}
		public void setEw(String ew) {
			this.ew = ew;
		}
		public String getNsLat() {
			return nsLat;
		}
		public void setNsLat(String nsLat) {
			this.nsLat = nsLat;
		}
		public String getEwLng() {
			return ewLng;
		}
		public void setEwLng(String ewLng) {
			this.ewLng = ewLng;
		}
	}
	
	private double toDouble(String value) {
		double dv = 0.0;
		try {
			dv = Double.parseDouble(value);
		} catch (NumberFormatException e) {
			LOG.error(value);
		}
		return dv;
	}
	
	// Ref: http://hi.baidu.com/superxiaoxin/blog/item/bb217afbcf9ed260034f56c1.html
	private void translateCoords(String ns, String ew, 
			String ddLat, String mmLat, String ssLat, 
			String ddLng, String mmLng, String ssLng, CoordDocObj obj) {
		try {
			double latFactor = 1.0;
			double lngFactor = 1.0;
			if(ns.toUpperCase().equals("N") || ns.toUpperCase().equals("S")) {
				obj.ns = ns.toUpperCase();
				if(obj.ns.equals("S")) {
					latFactor = -1.0;
				}
				
			} 
			if(ew.toUpperCase().equals("E") || ew.toUpperCase().equals("W")) {
				obj.ew = ew.toUpperCase();
				if(obj.ew.equals("W")) {
					lngFactor = -1.0;
				}
			}
			obj.nsLat = ddLat + "|" + mmLat + "|" + ssLat;
			obj.ewLng = ddLng + "|" + mmLng + "|"+ ssLng;
			double lat = latFactor * (Double.parseDouble(ddLat) + Double.parseDouble(mmLat)/60.0 + Double.parseDouble(ssLat)/3600.0);
			double lon = lngFactor * (Double.parseDouble(ddLng) + Double.parseDouble(mmLng)/60.0 + Double.parseDouble(ssLng)/3600.0);
			obj.setLatitude(lat);
			obj.setLongitude(lon);
		} catch (NumberFormatException e) {
			LOG.error("ddLat=" + ddLat + ", mmLat=" + mmLat + ", ssLat=" + ssLat + ";ddLng=" + ddLng + ", mmLngt=" + mmLng + ", ssLng=" + ssLng);
//			throw e;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void insert(CoordDocObj obj) {
		Map map = new HashMap();
		if(obj.getId()!=null) {
			map.put("_id", obj.getId());
		}
		if(obj.getTitle()!=null) {
			map.put("title", obj.getTitle());
		}
		if(obj.getLongitude()!=null || obj.getLatitude()!=null) {
			Map coord = new HashMap();
			if(obj.getLatitude()!=null) {
				coord.put("0", obj.getLatitude());
			}
			if(obj.getLongitude()!=null) {
				coord.put("1", obj.getLongitude());
			}
			map.put("center", coord);
		}
		if(obj.getType()!=null) {
			map.put("type", obj.getType());
		}
		if(obj.getEnglishName()!=null) {
			map.put("englishName", obj.getEnglishName());
		}
		if(obj.getDescription()!=null) {
			map.put("description", obj.getDescription());
		}
		if(obj.getContent()!=null) {
			map.put("content", obj.getContent());
		}
		
		Map origin = new HashMap();
		Map coordMap = new HashMap();
		CoordDocObj coord = (CoordDocObj)obj;
		if(coord.nsLat!=null) {
			String lat = coord.nsLat;
			if(coord.ns!=null) {
				lat += "|" + coord.ns;
			}
			coordMap.put("lat", lat);
		}
		if(coord.ewLng!=null) {
			String lng = coord.ewLng;
			if(coord.ew!=null) {
				lng += "|" + coord.ew;
			}
			coordMap.put("lng", lng);
		}
		if(!coordMap.isEmpty()) {
			origin.put("coord", coordMap);
			map.put("origin", origin);
		}
		DBObject o = new BasicDBObject(map);
		collection.insert(o);
		LOG.debug(obj);
	}
	
	public void runWork(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		StringBuffer xml = new StringBuffer();
		String line = "";
		while((line=reader.readLine())!=null) {
			if(line.trim().equals("</siteinfo>")) {
				break;
			}
		}
		while((line=reader.readLine())!=null) {
			xml.append(jc.t2s(line));
			xml.append("\r\n");
			if(line.trim().equals("</page>")) {
				parse(stringToStream(xml.toString()));
				xml =  new StringBuffer();
			}
		}
	}
	
	Pattern chineseCharacterPattern = Pattern.compile("[\u4e00-\u9fa5]");
	
	

	public static void main(String[] args) throws IOException {
		
		if(args.length!=1) {
			System.out.println("Commmamds:\tcom.tripfm.ie.WikipediaExtractor <xmlfile> ");
			System.out.println("HELP: " +
					"\n xmlfile \t Wikipedia XML file." +
					"\ne.g. com.zijiyou.wiki.WikipediaExtractor sample10w.xml  ");
			//return;
		}
		
		WikipediaZhExtractor extractor = new WikipediaZhExtractor();
		String path = "select.xml";
		try {
			extractor.runWork(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
