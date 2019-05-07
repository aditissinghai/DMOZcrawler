import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class DMOZCrawler {

    private static HashSet<String> blockedCategories = new HashSet<>();
    private static HashSet<String> visitedCategory = new HashSet<>();
    private static HashMap<Integer, Category> categoriesMap = new HashMap<>();
    private static ArrayList<String> validURLsList = new ArrayList<>();
    private static Deque<Category> categoriesToCrawl = new ArrayDeque<>();
    private static final String BASE_URL = "http://dmoz-odp.org";

    /**
     * Static block to load block categories, and prefix of valid URLs as program runs.
     */
    static {
        try{
            InputStream inputStream = DMOZCrawler.class.getResourceAsStream("blocked_categories_list");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String currentLine;
            while ((currentLine = bufferedReader.readLine()) != null){
                blockedCategories.add(currentLine.toLowerCase());
                System.out.println("Blocked category added :: " + currentLine);
            }

            inputStream = DMOZCrawler.class.getResourceAsStream("valid_urls_prefix");
            inputStreamReader = new InputStreamReader(inputStream);
            bufferedReader = new BufferedReader(inputStreamReader);

            while ((currentLine = bufferedReader.readLine()) != null){
                validURLsList.add(currentLine.toLowerCase());
                System.out.println("Valid URLs added :: " + currentLine);
            }
        } catch (IOException e){
            System.err.println("Error reading blocked categories");
        }

    }

    /**
     * Start point of the program. Initializes connection with MongoDB. Iteratively begins crawling of DMOZ categories
     * from "http://dmoz-odp.org" and continues till all categories are visited. Process initially begins with
     * extracting categories on "http://dmoz-odp.org" page (top level) and further drills down in each category
     * extracted
     * @param args System arguments
     */
    public static void main(String[] args) {
        MongoDbConnection mongoDbConnection = new MongoDbConnection();
        DMOZCrawler dmozCrawler = new DMOZCrawler();
        try {
            dmozCrawler.crawlTopLevel(BASE_URL);
            dmozCrawler.crawlIteratively();
            mongoDbConnection.insertDocuments(categoriesMap.values());
        } catch (Exception e) {
            System.err.println("Error at level 1");
            e.printStackTrace();
        }

        // Extracts content of each category having siteURLs
        mongoDbConnection.updateContent();

        //Targets list of used categories and extracts and updates their content only.
//        mongoDbConnection.updateUsedCategoriesContent();
    }

    /**
     * Crawls each top level category extracted from first page using BFS approach.
     */
    private void crawlIteratively() {
        while(categoriesToCrawl.size() > 0){
            Category category = categoriesToCrawl.pollFirst();
            if(category == null)
                continue;

            if(visitedCategory.contains(category.url))
                continue;

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

    /**
     * Using DOM structure extracts content for current category
     * @param category Category to process
     * @param document Jsoup document containing connection to current category URL
     */
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

    /**
     * Using DOM structure extracts childen categories for current category and adds them into queue for crawling.
     * @param currentCategory Category to process
     * @param document Jsoup document containing connection to current category URL
     */
    private void getChildrenCategories(Category currentCategory, Document document) {
        Elements CategoriesElements = document.select(".cat-item").select("a[href]");

        int count = categoriesMap.size();
        for (Element categoryElement: CategoriesElements) {
            String categoryName = categoryElement.text();
            categoryName = categoryName.substring(0, categoryName.lastIndexOf(" "))
                    .replaceAll("\u00AD","");


            if(blockedCategories.contains(categoryName.toLowerCase()))
                continue;

            String url = categoryElement.attr("abs:href");
            if(!validURL(url))
                continue;
            System.out.println(currentCategory.name + " : " + url);
            Category category = new Category(++count, categoryName, url, currentCategory.level + 1);
            categoriesMap.put(count, category);
            categoriesToCrawl.addLast(category);
        }
    }

    /**
     * Crawls top level URLS (URLs on seed page) and adds them to queue to extract all children categories.
     * @param seedURL URL from where crawling begins
     * @throws IOException  Exception while establishing connection with seedURL
     */
    private void crawlTopLevel(String seedURL) throws IOException {
        Document document = Jsoup.connect(seedURL).get();
        Element categorySectionElement = document.select("#category-section").first();
        Elements CategoriesElements = categorySectionElement.select(".top-cat").select("a[href]");

        int count = categoriesMap.size();
        for (Element categoryElement: CategoriesElements) {
            String categoryName = categoryElement.text();

            if(blockedCategories.contains(categoryName.toLowerCase()))
                continue;

            String url = categoryElement.attr("abs:href");
            if(!validURL(url))
                continue;

            Category category = new Category(++count, categoryName, url, 0);
            categoriesMap.put(count, category);
            categoriesToCrawl.addLast(category);
        }

    }

    /**
     * Checks if URL is valid by linearly comparing with valid URLs list loaded in static block.
     * @param url
     * @return
     */
    private boolean validURL(String url){
        for(String prefix : validURLsList){
            if(url.toLowerCase().startsWith(prefix))
                return true;
        }
        return false;
    }

}
