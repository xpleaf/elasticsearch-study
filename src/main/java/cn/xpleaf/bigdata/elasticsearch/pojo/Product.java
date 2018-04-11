package cn.xpleaf.bigdata.elasticsearch.pojo;

import lombok.Data;

@Data
public class Product {
    private String name;
    private String author;
    private String version;
    private String url;

    public Product() {

    }

    public Product(String name, String author, String version, String url) {
        this.name = name;
        this.author = author;
        this.version = version;
        this.url = url;
    }
}
