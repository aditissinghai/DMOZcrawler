import com.google.gson.Gson;
import com.mongodb.client.*;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import org.bson.Document;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.List;

public class MongoDbConnection {

    MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
    MongoDatabase mongoDatabase = mongoClient.getDatabase("DMOZ");
    MongoCollection mongoCollection = mongoDatabase.getCollection("categories");
    Gson gson = new Gson();

    void insertDocuments(Collection<Category> categoryList){
        mongoCollection.drop();
        for(Category category : categoryList){
            String json = gson.toJson(category);
            Document document = Document.parse(json);
            mongoCollection.insertOne(document);
            System.out.println(json);
        }
    }

    void updateContent(){
        Document query = new Document();
//        selected_collection.find({'locations' : {'$exists' : False}}, no_cursor_timeout=True)
        query.put("$where", "this.siteURLs.length>0");
        FindIterable findIterable = mongoCollection.find(query);

//        System.out.println(findIterable.);
        int count = 0;
        for (Object doc: findIterable){
            System.out.println(doc);
            StringBuilder content = new StringBuilder();
            Document result = (Document) doc;
            int id = result.getInteger("id");
            if(id < 96)
                continue;
            List<String> siteURLs = (List<String>) result.get("siteURLs");
            for(String url :siteURLs){
                try {
                    String urlContent = getContent(url);
                    if(urlContent != null)
                        content.append(urlContent).append("\n");
                } catch (BoilerpipeProcessingException e) {
                    System.err.println("URL: " + url + " Message: " + e.getMessage());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            mongoCollection.updateOne(new Document("id", id),new Document("$set", new Document("categoryContent", content.toString())));
            count++;
        }
        System.out.println(count);
    }

    private String getContent(String url) throws BoilerpipeProcessingException, IOException {
        URL uri = new URL(url);
        HttpURLConnection connection = (HttpURLConnection)uri.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.connect();
        int code = connection.getResponseCode();
        System.out.println(url + " :: " + code);
        String content = null;
        if(code < 400) {
            content = ArticleExtractor.INSTANCE.getText(uri);
        }
        connection.disconnect();
        return content;
    }

}
