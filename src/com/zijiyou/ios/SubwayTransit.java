package com.zijiyou.ios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.redraccoon.dijkstra.model.Edge;
import com.redraccoon.dijkstra.model.Graph;
import com.zijiyou.mongo.MongoConnector;

public class SubwayTransit {
	private HashMap<String, Integer> stationIDMap = new HashMap<String, Integer>();
	private static HashMap<Integer, String> IDStationMap = new HashMap<Integer, String>();
	private static HashMap<String, String> IDNameMap = new HashMap<String, String>();
	private static HashMap<String,Set<Integer>>  IDLineMap=new HashMap<String,Set<Integer>>();
	private static HashMap<Integer,String>  lineColorMap=new HashMap<Integer,String>();
	private static HashMap<Integer,String>  lineNameMap=new HashMap<Integer,String>();
	private Graph graph = null;
	private String regionID;
	
	Connection con=SQLite.getConnection("transfer4.db");

	public SubwayTransit(String regionID) throws Exception {
		this.regionID = regionID;
		
        Statement stat = con.createStatement();
        String ddl="create table if not exists transfer(" +
        		"sourcestation varchar(20)," +
        		"destinationstation varchar(20), " +
        		"stationcount integer," +
        		"stationlist varchar(1000)" +
        		")";
        stat.execute(ddl);
	}

	//get subwaypoiid-->Integer
	public void initializeSubway() {

		MongoConnector mgc = new MongoConnector("analyzer.properties",
				"mongo_tripfm");

		DBCollection poiCollection = mgc.getDB().getCollection("POI");
		DBObject qpoi = new BasicDBObject().append("category", "subway").append("regionId",new ObjectId(regionID));
		DBCursor pCursor = poiCollection.find(qpoi);

		while (pCursor.hasNext()) {
			DBObject dbo = pCursor.next();
			String poiid = dbo.get("_id").toString();
			String name = dbo.get("name").toString();
			IDNameMap.put(poiid, name);
			
			Set<Integer> lineNumberSet=new HashSet<Integer>();
			if(dbo.get("subway") instanceof com.mongodb.BasicDBList){
				BasicDBList lineList=(BasicDBList)dbo.get("subway");
				for(Object line:lineList){
					BasicDBObject linedbo=(BasicDBObject)line;
					lineNumberSet.add(Integer.parseInt(linedbo.getString("lineId")));
				}
				IDLineMap.put(poiid, lineNumberSet);
			}else{
				System.out.println("Wrong format: "+name+"  "+dbo);
				
			}
		}

		DBCollection lineCollection = mgc.getDB().getCollection("Subway");
		DBCursor lineCursor = lineCollection.find(new BasicDBObject("regionId",new ObjectId(regionID)));
		List<Edge> edges = new ArrayList<Edge>();
		Integer idSequence = 0;
		Integer currentID = 0;
		while (lineCursor.hasNext()) {
			DBObject dbo = lineCursor.next();
			Integer lineid = Integer.parseInt(dbo.get("lineId").toString());
			String color = dbo.get("color").toString();
			lineColorMap.put(lineid, color);
			lineNameMap.put(lineid,dbo.get("name").toString());
			List<BasicDBObject> subLineList = (List<BasicDBObject>) dbo
					.get("subline");
			List<BasicDBObject> newStationList = new ArrayList<BasicDBObject>();
			for (BasicDBObject bdbo : subLineList) {
				Integer previousID = 0;
				List<BasicDBObject> stationList=(List<BasicDBObject>)bdbo.get("list");
				for(BasicDBObject stationDbo:stationList){
					String poiid = stationDbo.getString("poiId");
					if (stationIDMap.containsKey(poiid)) {
						currentID = stationIDMap.get(poiid);
					} else {
						stationIDMap.put(poiid, ++idSequence);
						IDStationMap.put(idSequence, poiid);
						currentID = idSequence;
					}

					if (previousID > 0) {
						Edge edge = new Edge(currentID, previousID, 1);
						edges.add(edge);
					}
					previousID = currentID;
				}
			}
		}
		// 香港站和中环站可以链接
		Edge edge=  new Edge(2, 45,4); 
		edges.add(edge);
		Edge[] edgearray = new Edge[edges.size()];
		
		graph = new Graph(edges.toArray(edgearray));
	}

	public static Set<Integer> joinSet(Set<Integer> one, Set<Integer> two) {
		Set<Integer> resultSet = new HashSet<Integer>();
		for (Integer id : one) {
			if (two.contains(id))
				resultSet.add(id);

		}
		return resultSet;

	}

	public  void writeSubway(int sourcStation) throws SQLException {

		HashMap<Integer, Vector<Integer>> routeMap= this.graph
				.calculateShortestDistances(sourcStation);
		System.out.println(IDStationMap.get(sourcStation)+" "+IDNameMap.get(IDStationMap.get(sourcStation)));
		PreparedStatement insertStmt=con.prepareStatement("insert into transfer(sourcestation,destinationstation,stationcount,stationlist) " +
        		"values(?,?,?,?)");
		
		for (Map.Entry<Integer, Vector<Integer>> entry : routeMap.entrySet()) {

			String destinationStationID = IDStationMap.get(entry.getKey());
			String destinationStationName = destinationStationID+"   "+entry.getKey()+"|"
					+ IDNameMap.get(destinationStationID);

			// step1 :将路线中的地铁站张按照顺序利用Map存储 order-->poiid
			HashMap<Integer, String> stationOrderMap = new HashMap<Integer, String>();
			stationOrderMap.put(0,IDStationMap.get(sourcStation) );  //将sourcestation加入到stationOrderMap
			int j = 1;
			for (Integer id : entry.getValue()) {
				String stationPOI = IDStationMap.get(id);
				stationOrderMap.put(j++, stationPOI);
			}

			if(stationOrderMap.keySet().size()> 5)
				System.out.println("catched");
					
			//Setp2 : 确定每个地铁站对应的线路是否是换乘
			//某个站点和之前站点共同经过的线路 sameLine 该站点和下一个站共同经过的线路nextStationline
			//sameStationLine和nextStationLine完全不同，则该站点一定为换乘站
			//sameLine为上一换乘站到该换乘站经历的线路。
			HashMap<Integer, Set<Integer>> stationRouteLineMap = new HashMap<Integer, Set<Integer>>();
			// 记录换乘结点出现之前所有地铁站共同出现的线路
			// 用来记最终的某个站点的线路
			Set<Integer> sameLines = new HashSet<Integer>();
			Set<Integer> unconfirmStationOrders = new HashSet<Integer>();

			for (Integer id=0;id<j;id++) {
				Set<Integer> currentLines = IDLineMap.get(
						stationOrderMap.get(id));
				
				if (!sameLines.isEmpty()) {
					sameLines = joinSet(sameLines, currentLines);
				} else
					sameLines = currentLines;

				if (stationOrderMap.get(id + 1) != null) {
					Set<Integer> nextStationLine = IDLineMap.get(
							stationOrderMap.get(id + 1));//.keySet();
					
					if (joinSet(sameLines, nextStationLine) == null
							|| joinSet(sameLines, nextStationLine).isEmpty()) {
						for (Integer stationOrder : unconfirmStationOrders) {
							stationRouteLineMap.put(stationOrder, sameLines);
						}
						HashSet<Integer> transferLineSet = new HashSet<Integer>();
						transferLineSet.add(0);
						stationRouteLineMap.put(id, transferLineSet);
						sameLines = currentLines;
						unconfirmStationOrders.clear();

					} else
						unconfirmStationOrders.add(id);
				} else { // 到最后一个站点了
					unconfirmStationOrders.add(id);
					for (Integer stationOrder : unconfirmStationOrders) {
						stationRouteLineMap.put(stationOrder, sameLines);
					}
				}
			}
			
			//Step3: 将确定好的换乘列表转成json格式
			List<BasicDBObject> stationList=new ArrayList<BasicDBObject>();
			String strStations="";
			for (Integer id : stationOrderMap.keySet()) {
				BasicDBObject dbo=new BasicDBObject();
				dbo.append("p", stationOrderMap.get(id));
				Integer lineid=0;
				if(stationRouteLineMap.containsKey(id)&&stationRouteLineMap.get(id).size()>0)
					 lineid=stationRouteLineMap.get(id).iterator().next();
				else
					 System.out.println("missing lineid");
				
				dbo.append("line",lineid);
				strStations=strStations+IDNameMap.get(stationOrderMap.get(id))+lineid;
				stationList.add(dbo);
			}
			insertStmt.setString(2, destinationStationID);
			insertStmt.setString(1, IDStationMap.get(sourcStation));
			insertStmt.setInt(3, stationList.size());
			insertStmt.setString(4, stationList.toString());
			insertStmt.execute();
			System.out.println(destinationStationName + ": " + strStations);
		}

	}

	public static void main(String args[]) throws Exception {
		
		SubwayTransit sbt = new SubwayTransit("4e8c091fd0c2ff482300031d");
		for(int j=1;j<85;j++){
			sbt.initializeSubway();
			sbt.writeSubway(j);
			
		}
	}

}
