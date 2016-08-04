package com.crawl.marklines;

import java.net.UnknownHostException;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class T {

	public static void main(String[] args) throws UnknownHostException, MongoException {

	}

	// @SuppressWarnings("unchecked")
	// public static void loadMediaTags(List<MediaEntity> mediaEntityList) {
	// mediaEntityList.clear();
	//
	// try {
	// Mongo mongo = new Mongo("localhost", 27017);
	// DB db = mongo.getDB("db");
	// DBCollection collection = db.getCollection("tb");
	//
	// DBCursor cursor = collection.find();
	// int index = 0;
	// long startTime = System.currentTimeMillis();
	// while (cursor.hasNext()) {
	// BasicDBObject obj = (BasicDBObject) cursor.next();
	//
	// long id = obj.getLong("_id");
	// ArrayList<String> tagList = (ArrayList<String>) obj.get("tag"); // tag
	// ArrayList<String> keywordList = (ArrayList<String>) obj.get("keyword"); // keyword
	// ArrayList<Integer> copyrightList = (ArrayList<Integer>) obj.get("copyright"); // copyright
	//
	// MediaEntity mediaEntity = new MediaEntity();
	//
	// mediaEntity.setId(id);
	//
	// // tag
	// for (int j = 0; j < tagList.size(); j++) {
	// mediaEntity.addTag(tagList.get(j));
	// int tagId = getTagInt(tagList.get(j));
	// mediaEntity.addTag(tagId);
	// }
	//
	// // actors
	// ArrayList<DBObject> actorsObjList = (ArrayList<DBObject>) obj.get("actors"); // actors
	// for (int j = 0; j < actorsObjList.size(); j++) {
	// mediaEntity.addActor((String) actorsObjList.get(j).get("name"));
	// int actorId = getActorInt((String) actorsObjList.get(j).get("name"));
	// mediaEntity.addActor(actorId);
	// }
	// // director
	// ArrayList<DBObject> directorObjList = (ArrayList<DBObject>) obj.get("director"); // director
	// for (int j = 0; j < directorObjList.size(); j++) {
	// mediaEntity.addDirector((String) directorObjList.get(j).get("name"));
	// int directorId = getDirectorInt((String) directorObjList.get(j).get("name"));
	// mediaEntity.addDirector(directorId);
	// }
	//
	// // keyword
	// for (int j = 0; j < keywordList.size(); j++) {
	// mediaEntity.addKeyword(keywordList.get(j));
	// int keywordId = getKeywordInt(keywordList.get(j));
	// mediaEntity.addKeyword(keywordId);
	// }
	//
	// // copyright
	// for (int j = 0; j < copyrightList.size(); j++) {
	// mediaEntity.addCopyright(copyrightList.get(j));
	// }
	//
	// mediaEntityList.add(mediaEntity);
	//
	// index++;
	// if (index > 100) {
	// break;
	// }
	// System.out.println(index + " --- mediaEntity : " + mediaEntity.toString());
	// }
	// long costTime = System.currentTimeMillis() - startTime;
	// System.out.println("load data costTime = " + index + "; costTime = " + costTime / 1000f);
	//
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }

	public static int getTagInt(String tag) {
		int tagIntId = -1;

		MongoClient mongo = new MongoClient("localhost", 27017);
		MongoDatabase db = mongo.getDatabase("db");
		MongoCollection<Document> tagmapCollection = db.getCollection("recommend_tag_map");
		FindIterable<Document> cursor = tagmapCollection.find(new BasicDBObject("name", tag));
		if (cursor == null || cursor.first() != null) { // 处理小于2或n的映射关键字，下同
			return tagIntId;
		}
		MongoCursor<Document> cur = cursor.iterator();
		// 取出结果
		while (cur.hasNext()) {
			Document doc = (Document) cur.next();
			System.out.println("---------------------------------------------------------------------------------");
			System.out.println(doc.toJson());
			System.out.println(doc.get("_id"));
			doc.clear();
		}

		Document obj = cursor.first();

		String name = tag;
		tagIntId = (Integer) obj.get("id");
		int num = (Integer) obj.get("num");

		cur.close();
		mongo.close();

		return tagIntId;
	}

	public static int getActorInt(String actor) {
		int actorIntId = -1;
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("db");
		DBCollection tagmapCollection = db.getCollection("recommend_actor_map");
		DBCursor cursor = tagmapCollection.find(new BasicDBObject("name", actor));
		if (cursor == null || cursor.toArray().size() <= 0) {
			return actorIntId;
		}
		DBObject obj = cursor.toArray().get(0);
		String name = actor;
		actorIntId = (Integer) obj.get("id");
		int num = (Integer) obj.get("num");
		mongo.close();
		return actorIntId;
	}
}
