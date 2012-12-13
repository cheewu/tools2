package com.zijiyou.ios;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;


public class TileFileCrawler {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Set<String> tileIDSet =new HashSet<String>();
		String dbFileDir="/Users/cheewu/Dropbox/mbtiles/";
		Connection con = SQLite.getConnection(dbFileDir+"hongkong/osm_hk.mbtiles");
		java.sql.Statement stmt=con.createStatement();
		ResultSet tileRs=stmt.executeQuery("select tile_ID from images");
		while(tileRs.next()){
			tileIDSet.add(tileRs.getString(1));
		}
		tileRs.close();
		ResultSet mapRs=stmt.executeQuery("select zoom_level,tile_column,tile_row,tile_id from map");
		PreparedStatement instStmt = con
				.prepareStatement("insert into images(tile_id,tile_data) values(?,?) ");
		int j=0;
		while(mapRs.next()){
			j++;
			int level=mapRs.getInt(1);
			int cloumn=mapRs.getInt(2);
			int row=mapRs.getInt(3);
			String tileID=mapRs.getString(4);
			if(tileIDSet.contains(tileID)){
				System.out.println(j+ "already exist!");
				continue;
			}
			else
				tileIDSet.add(tileID);
			instStmt.setString(1, tileID);
			instStmt.setBytes(2, downloadImage(level,cloumn,row,tileID));
			instStmt.execute();
			System.out.println(j+ "images has been updated"+tileID);
		}
		
	}

	
	public static byte[] downloadImage(String url,String directory,String fileName) throws IOException, InterruptedException{
		
		String imageFileName= directory+"/"+fileName;
		if(new File(imageFileName).exists()){
			System.out.println("Image file exists:"+ imageFileName);
			return  SQLite.readImage(imageFileName);
		}
		
		URL server = new URL(url);
	    HttpURLConnection connection = (HttpURLConnection)server.openConnection();
	    connection.setRequestMethod("GET");
	    connection.setDoInput(true);
	    connection.setDoOutput(true);
	    connection.setUseCaches(false);
	    connection.addRequestProperty("Accept","image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/msword, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/x-shockwave-flash, */*");
	    connection.addRequestProperty("Accept-Language", "en-us,zh-cn;q=0.5");
	    connection.addRequestProperty("Accept-Encoding", "gzip, deflate");
	    connection.addRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; .NET CLR 2.0.50727; MS-RTC LM 8)");
	    connection.connect();
	    InputStream is = connection.getInputStream();		
	    OutputStream os = new FileOutputStream(imageFileName);
	    Thread.sleep(1000);		
	    byte[] buffer = new byte[1024];		
	    int byteReaded = is.read(buffer);
	    while(byteReaded != -1)
	    {
	        os.write(buffer,0,byteReaded);
	        byteReaded = is.read(buffer);
	    }
	   os.close();
	   return SQLite.readImage(imageFileName);

	}
	
	
	public static byte[] downloadImage(int level,int column,int row,String tileID) throws Exception {
		
		row=(int)(Math.pow(2, level)-row-1);
		
		String imgURL="http://maps.zijiyou.com/v3/cheewu.map-r23texn3/"+level+"/"+column+"/"+row+".png";
		
		return downloadImage(imgURL,"mbtileshk",tileID+".png");
		

	}

}
