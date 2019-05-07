import com.google.gson.Gson;
import com.mongodb.client.*;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class MongoDbConnection {

    MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
    MongoDatabase mongoDatabase = mongoClient.getDatabase("DMOZ");
    MongoCollection mongoCollection = mongoDatabase.getCollection("categories");
    Gson gson = new Gson();

    /**
     * Given a list of categories, the function inserts them into MongoDB - DMOZ database - categories collection
     * @param categoryList List of categories to insert
     */
    void insertDocuments(Collection<Category> categoryList){
        mongoCollection.drop();
        for(Category category : categoryList){
            String json = gson.toJson(category);
            Document document = Document.parse(json);
            mongoCollection.insertOne(document);
            System.out.println(json);
        }
    }

    /**
     * Collects all categories where categories have no content and have siteURLs to extract content from. Iteratively
     * extracts content from each URL and update it category respectively.
     * Note: This function uses boiler pipe to get filtered content from a URL.
     */
    void updateContent(){
        Document query = new Document();
//        selected_collection.find({'locations' : {'$exists' : False}}, no_cursor_timeout=True)
        query.put("$where", "this.categoryContent.length==0 & this.siteURLs.length>0");
        FindIterable findIterable = mongoCollection.find(query);

        int count = 0;
        for (Object doc: findIterable){
            System.out.println(doc);
            StringBuilder content = new StringBuilder();
            Document result = (Document) doc;
            int id = result.getInteger("id");
            List<String> siteURLs = (List<String>) result.get("siteURLs");
            int urlsCount = 0;
            for(String url :siteURLs){
                try {
                    String urlContent = getContent(url);
                    if(urlContent != null){
                        content.append(urlContent).append("\n");
                        urlsCount++;
                        if (urlsCount >= 5)
                            break;
                    }
                } catch (BoilerpipeProcessingException e) {
                    System.err.println("URL: " + url + " Message: " + e.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            mongoCollection.updateOne(new Document("id", id),new Document("$set", new Document("categoryContent", content.toString())));
            count++;
        }
        System.out.println(count);
    }

    /**
     * Given a URL, this function creates a HTTP connection with URL and extracts its content using Boiler pipe.
     * Note: If URL is redirected this function keeps on moving forward with redirected location till status code is
     * different from 301.
     * @param url
     * @return
     * @throws BoilerpipeProcessingException
     * @throws IOException
     */
    private String getContent(String url) throws BoilerpipeProcessingException, IOException {
        int code;
        HttpURLConnection connection = null;
        URL uri;
        do{
            uri = new URL(url);
            if(connection != null)
                connection.disconnect();
            connection = (HttpURLConnection)uri.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();
            code = connection.getResponseCode();
            if(code == 301)
                url = connection.getHeaderField( "Location" );
        }while (code == 301);

        System.out.println(url + " :: " + code);
        String content = null;
        if(code < 400) {
            content = ArticleExtractor.INSTANCE.getText(uri);
        }
        connection.disconnect();
        return content;
    }

    /**
     * This function updates content of categories listed in Used_categories.txt
     */
    void updateUsedCategoriesContent() {
        try{
            InputStream inputStream = DMOZCrawler.class.getResourceAsStream("Used_categories.txt");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            HashSet<String> usedCategories = new HashSet<>();

            String currentLine;
            while ((currentLine = bufferedReader.readLine()) != null){
                usedCategories.add(currentLine.toLowerCase());
                System.out.println("Used category added :: " + currentLine);
            }

            System.out.println(usedCategories);

            for(String id : usedCategories){
                categoryContent(Integer.parseInt(id));
            }
        } catch (Exception exception){
            exception.printStackTrace();
        }
    }

    /**
     * This function targets categories listed in Used_categories.txt file for updating the content in MongoDB.
     * @param categoryId
     */
    void categoryContent(Integer categoryId){
        Document query = new Document();
//        selected_collection.find({'locations' : {'$exists' : False}}, no_cursor_timeout=True)
        query.put("id", categoryId);
        query.put("$where", "this.categoryContent.length==0");
        FindIterable findIterable = mongoCollection.find(query);

        int count = 0;
        for (Object doc: findIterable){
            System.out.println(doc);
            StringBuilder content = new StringBuilder();
            Document result = (Document) doc;
            int id = result.getInteger("id");
            List<String> siteURLs = (List<String>) result.get("siteURLs");
            for(String url :siteURLs){
                try {
                    String urlContent = getContent(url);
                    if(urlContent != null){
                        content.append(urlContent).append("\n");
                    }
                } catch (BoilerpipeProcessingException e) {
                    System.err.println("URL: " + url + " Message: " + e.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            mongoCollection.updateOne(new Document("id", id),new Document("$set", new Document("categoryContent", content.toString())));
            count++;
        }
        System.out.println(count);
    }

}
