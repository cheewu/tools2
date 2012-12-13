package com.zijiyou.mongo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoConnector {

	private static final Logger LOG = Logger.getLogger(MongoConnector.class);
	private Mongo mongo;
	private DB db;

	public MongoConnector(String host, int port) {
		if (mongo == null) {
			if (LOG.isInfoEnabled())
				LOG.info("Mongo not exist ,try to build one new connection！");
			mongo = MongoConnector.buildConnection(host, port);
		}

	}

	public MongoConnector(String propsPath, String dbProperty) {

		Properties props = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream("analyzer.properties");
			props.load(fis);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String mongoHost = props.getProperty("mongo_host");
		int mongoPort = Integer.parseInt(props.getProperty("mongo_port"));
		String mongoUser = props.getProperty("mongo_user");
		String mongoPasswd = props.getProperty("mongo_passwd");
		String mongodb = props.getProperty(dbProperty);
		boolean auth = Boolean.parseBoolean(props.getProperty("mongo_auth"));
		if (mongo == null) {
			if (LOG.isInfoEnabled())
				LOG.info("Mongo not exist ,try to build one new connection！");
			mongo = MongoConnector.buildConnection(mongoHost, mongoPort);
		}

		DB db = mongo.getDB(mongodb);
		this.db = db;

		if (auth) {
			if (db.isAuthenticated()) {
				this.db = db;

			} else {
				boolean login = db.authenticate(mongoUser,
						mongoPasswd.toCharArray());
				if (login)
					this.db = db;
				else {
					this.db = null;
				}
			}

		}

	}


	public DB getDB() {
		return db;
	}

	public DB getDB(String dbname, String user, String passwd) {
		DB db = mongo.getDB(dbname);
		if (db.isAuthenticated())
			return db;
		boolean login = db.authenticate(user, passwd.toCharArray());
		if (login)
			return db;
		else {
			return null;
		}
	}

	private static Mongo buildConnection(String host, int port) {
		Mongo mongo = null;
		try {
			mongo = new Mongo(host, port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		return mongo;
	}

	public void close() {
		if (mongo != null) {
			mongo.close();
		}
	}

}
