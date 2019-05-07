import java.util.ArrayList;
import java.util.Collection;

/**
 * An object class for representing a category.
 */
public class Category {
    int id;
    String name;
    ArrayList<Integer> categoryRoadMap;
    ArrayList<String> siteURLs;
    String url;
    int level;
    String categoryContent;

    Category(int id, String name, String url, int level) {
        this.id = id;
        this.name = name;
        this.url = url;
        this.level = level;
        categoryRoadMap = new ArrayList<>();
        siteURLs = new ArrayList<>();
        categoryContent = "";
    }

    void addCategoryToCategoryRoadMap(int categoryId) {
        categoryRoadMap.add(categoryId);
    }

    void addAllCategoryToCategoryRoadMap(Collection<Integer> categoryIds) {
        categoryRoadMap.addAll(categoryIds);
    }

    void addContentURL(String siteURL) {
        siteURLs.add(siteURL);
    }

    String getContent(){
        return categoryContent;
    }

    @Override
    public String toString() {
        return id +" - " + name + " : " + url;
    }
}
