import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;

public class DMOZCrawler {

    private static HashSet<String> blockedCategories = new HashSet<>();
    private static HashSet<String> visitedCategory = new HashSet<>();
    private static HashMap<Integer, Category> categoriesMap = new HashMap<>();
    private static Deque<Category> categoriesToCrawl = new ArrayDeque<>();
    private static final String BASE_URL = "http://dmoz-odp.org";

    static {
        try{
            InputStream inputStream = DMOZCrawler.class.getResourceAsStream("blocked_categories_list");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String currentLine;
            while ((currentLine = bufferedReader.readLine()) != null){
                blockedCategories.add(currentLine);
                System.out.println("Blocked category added :: " + currentLine);
            }
        } catch (IOException e){
            System.err.println("Error reading blocked categories");
        }

    }

    public static void main(String[] args) {
        MongoDbConnection mongoDbConnection = new MongoDbConnection();
        DMOZCrawler dmozCrawler = new DMOZCrawler();
        try {
            dmozCrawler.crawlTopLevel(BASE_URL);
            dmozCrawler.crawlRecursively();
            mongoDbConnection.insertDocuments(categoriesMap.values());
        } catch (IOException e) {
            System.err.println("Error at level 1");
            e.printStackTrace();
        }

        mongoDbConnection.updateContent();
    }

    private void crawlRecursively() {
        while(categoriesToCrawl.size() > 0){
            Category category = categoriesToCrawl.pollFirst();
            if(category == null)
                continue;

            if(visitedCategory.contains(category.url))
                return;

            visitedCategory.add(category.url);

            try {
                System.out.println("Crawling: " + category.url);
                Document document = Jsoup.connect(category.url).get();
                getChildrenCategories(category, document);
                getContentForCurrentCategory(category, document);
            } catch (IOException e) {
                System.err.println("Error retrieving category :: " + category.name);
                System.err.println("Error :: " + e.getMessage());
            }
        }
    }

    private void getContentForCurrentCategory(Category category, Document document) {
        Element categorySectionElement = document.select("#site-list-content").first();
        if(categorySectionElement == null)
            return;
        Elements siteElements = categorySectionElement.select(".title-and-desc").select("a[href]");
        for (Element siteElement: siteElements) {
            String url = siteElement.attr("abs:href");
            category.addContentURL(url);
        }
    }

    private void getChildrenCategories(Category currentCategory, Document document) {
        Element categorySectionElement = document.select("#cat-list-content-main").first();
        if(categorySectionElement == null)
            return;
        Elements CategoriesElements = categorySectionElement.select(".cat-item").select("a[href]");

        int count = categoriesMap.size();
        for (Element categoryElement: CategoriesElements) {
            String categoryName = categoryElement.text();
            categoryName = categoryName.substring(0, categoryName.lastIndexOf(" "))
                    .replaceAll("\u00AD","");


            if(blockedCategories.contains(categoryName))
                continue;

            String url = categoryElement.attr("abs:href");
            Category category = new Category(++count, categoryName, url, currentCategory.level + 1);
            categoriesMap.put(count, category);
            categoriesToCrawl.addLast(category);
        }
    }

    private void crawlTopLevel(String seedURL) throws IOException {
        Document document = Jsoup.connect(seedURL).get();
        Element categorySectionElement = document.select("#category-section").first();
        Elements CategoriesElements = categorySectionElement.select(".top-cat").select("a[href]");

        int count = categoriesMap.size();
        for (Element categoryElement: CategoriesElements) {
            String categoryName = categoryElement.text();

            if(blockedCategories.contains(categoryName))
                continue;

            String url = categoryElement.attr("abs:href");
            Category category = new Category(++count, categoryName, url, 0);
            categoriesMap.put(count, category);
            categoriesToCrawl.addLast(category);
        }

    }

}
