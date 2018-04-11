package cn.xpleaf.bigdata.elasticsearch;

import cn.xpleaf.bigdata.elasticsearch.pojo.Product;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 使用Java API来操作es集群
 * Transport
 * 代表了一个集群
 * 我们客户端和集群通信是使用TransportClient
 */
public class ElasticSearchTest {

    private TransportClient client;
    private String index = "bigdata";
    private String type = "product";

    @Before
    public void setup() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "bigdata-08-28").build();
        client = TransportClient.builder().settings(settings).build();
        TransportAddress ta1 = new InetSocketTransportAddress(InetAddress.getByName("uplooking01"), 9300);
        TransportAddress ta2 = new InetSocketTransportAddress(InetAddress.getByName("uplooking02"), 9300);
        TransportAddress ta3 = new InetSocketTransportAddress(InetAddress.getByName("uplooking03"), 9300);
        client.addTransportAddresses(ta1, ta2, ta3);
        /*settings = client.settings();
        Map<String, String> asMap = settings.getAsMap();
        for(Map.Entry<String, String> setting : asMap.entrySet()) {
            System.out.println(setting.getKey() + "::" + setting.getValue());
        }*/
    }

    /**
     * 注意：往es中添加数据有4种方式
     * 1.JSON
     * 2.Map
     * 3.Java Bean
     * 4.XContentBuilder
     *
     * 1.JSON方式
     */
    @Test
    public void testAddJSON() {
        String source = "{\"name\":\"sqoop\", \"author\": \"apache\", \"version\": \"1.4.6\"}";
        IndexResponse response = client.prepareIndex(index, type, "4").setSource(source).get();
        System.out.println(response.isCreated());
    }

    /**
     * 添加数据：
     * 2.Map方式
     */
    @Test
    public void testAddMap() {
        Map<String, Object> source = new HashMap<String, Object>();
        source.put("name", "flume");
        source.put("author", "Cloudera");
        source.put("version", "1.8.0");
        IndexResponse response = client.prepareIndex(index, type, "5").setSource(source).get();
        System.out.println(response.isCreated());
    }

    /**
     * 添加数据：
     * 3.Java Bean方式
     *
     * 如果不将对象转换为json字符串，则会报下面的异常：
     * The number of object passed must be even but was [1]
     */
    @Test
    public void testAddObj() throws JsonProcessingException {
        Product product = new Product("kafka", "linkedIn", "0.10.0.1", "kafka.apache.org");
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(product);
        System.out.println(json);
        IndexResponse response = client.prepareIndex(index, type, "6").setSource(json).get();
        System.out.println(response.isCreated());
    }

    /**
     * 添加数据：
     * 4.XContentBuilder方式
     */
    @Test
    public void testAddXContentBuilder() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder();
        source.startObject()
                .field("name", "redis")
                .field("author", "redis")
                .field("version", "3.2.0")
                .field("url", "redis.cn")
                .endObject();
        IndexResponse response = client.prepareIndex(index, type, "7").setSource(source).get();
        System.out.println(response.isCreated());
    }

    /**
     * 查询具体的索引信息
     */
    @Test
    public void testGet() {
        GetResponse response = client.prepareGet(index, type, "6").get();
        Map<String, Object> map = response.getSource();
        /*for(Map.Entry<String, Object> me : map.entrySet()) {
            System.out.println(me.getKey() + "=" + me.getValue());
        }*/
        // lambda表达式，jdk 1.8之后
        map.forEach((k, v) -> System.out.println(k + "=" + v));
//        map.keySet().forEach(key -> System.out.println(key + "xxx"));
    }

    /**
     * 局部更新操作与curl的操作是一致的
     * curl -XPOST http://uplooking01:9200/bigdata/product/AWA184kojrSrzszxL-Zs/_update -d' {"doc":{"name":"sqoop", "author":"apache"}}'
     *
     * 做全局更新的时候，也不用prepareUpdate，而直接使用prepareIndex
     */
    @Test
    public void testUpdate() throws Exception {
        /*String source = "{\"doc\":{\"url\": \"http://flume.apache.org\"}}";
        UpdateResponse response = client.prepareUpdate(index, type, "4").setSource(source.getBytes()).get();*/
        // 使用下面这种方式也是可以的
        String source = "{\"url\": \"http://flume.apache.org\"}";
        UpdateResponse response = client.prepareUpdate(index, type, "4").setDoc(source.getBytes()).get();
        System.out.println(response.getVersion());
    }

    /**
     * 删除操作
     */
    @Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete(index, type, "5").get();
        System.out.println(response.getVersion());
    }

    /**
     * 批量操作
     */
    @Test
    public void testBulk() {
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type, "8")
                .setSource("{\"name\":\"elasticsearch\", \"url\":\"http://www.elastic.co\"}");
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, "1").setDoc("{\"url\":\"http://hadoop.apache.org\"}");
        BulkRequestBuilder bulk = client.prepareBulk();
        BulkResponse bulkResponse = bulk.add(indexRequestBuilder).add(updateRequestBuilder).get();
        Iterator<BulkItemResponse> it = bulkResponse.iterator();
        while(it.hasNext()) {
            BulkItemResponse response = it.next();
            System.out.println(response.getId() + "<--->" + response.getVersion());
        }
    }

    /**
     * 获取索引记录数
     */
    @Test
    public void testCount() {
        CountResponse response = client.prepareCount(index).get();
        System.out.println("索引记录数：" + response.getCount());
    }

    @After
    public void cleanUp() {
        client.close();
    }
}
