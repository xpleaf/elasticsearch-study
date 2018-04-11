package cn.xpleaf.bigdata.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * 使用Java API来操作es集群
 * Transport
 * 代表了一个集群
 * 我们客户端和集群通信是使用TransportClient
 * <p>
 * 使用prepareSearch来完成全文检索之
 *  中文分词
 */
public class ElasticSearchTest3 {

    private TransportClient client;
    private String index = "bigdata";
    private String type = "product";
    private String[] indics = {"chinese"};

    @Before
    public void setUp() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "bigdata-08-28").build();
        client = TransportClient.builder().settings(settings).build();
        TransportAddress ta1 = new InetSocketTransportAddress(InetAddress.getByName("uplooking01"), 9300);
        TransportAddress ta2 = new InetSocketTransportAddress(InetAddress.getByName("uplooking02"), 9300);
        TransportAddress ta3 = new InetSocketTransportAddress(InetAddress.getByName("uplooking03"), 9300);
        client.addTransportAddresses(ta1, ta2, ta3);
    }

    /**
     * 中文分词的操作
     * 1.查询以"中"开头的数据，有两条
     * 2.查询以“中国”开头的数据，有0条
     * 3.查询包含“烂”的数据，有1条
     * 4.查询包含“烂摊子”的数据，有0条
     * 分词：
     *      为什么我们搜索China is the greatest country~
     *                 中文：中国最牛逼
     *
     *                 中华人民共和国
     *                      中华
     *                      人民
     *                      共和国
     *                      中华人民
     *                      人民共和国
     *                      华人
     *                      共和
     *      特殊的中文分词法：
     *          庖丁解牛
     *          IK分词法
     *          搜狗分词法
     */
    @Test
    public void testSearch1() {
        SearchResponse response = client.prepareSearch(indics)    // 在prepareSearch()的参数为索引库列表，意为要从哪些索引库中进行查询
                .setSearchType(SearchType.DEFAULT)  // 设置查询类型，有QUERY_AND_FETCH  QUERY_THEN_FETCH  DFS_QUERY_AND_FETCH  DFS_QUERY_THEN_FETCH
                //.setQuery(QueryBuilders.prefixQuery("content", "烂摊子"))// 设置相应的query，用于检索，termQuery的参数说明：name是doc中的具体的field，value就是要找的具体的值
//                .setQuery(QueryBuilders.regexpQuery("content", ".*烂摊子.*"))
                .setQuery(QueryBuilders.prefixQuery("content", "中国"))
                .get();
        showResult(response);
    }

    /**
     * 格式化输出查询结果
     * @param response
     */
    private void showResult(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        float maxScore = searchHits.getMaxScore();  // 查询结果中的最大文档得分
        System.out.println("maxScore: " + maxScore);
        long totalHits = searchHits.getTotalHits(); // 查询结果记录条数
        System.out.println("totalHits: " + totalHits);
        SearchHit[] hits = searchHits.getHits();    // 查询结果
        System.out.println("当前返回结果记录条数：" + hits.length);
        for (SearchHit hit : hits) {
            long version = hit.version();
            String id = hit.getId();
            String index = hit.getIndex();
            String type = hit.getType();
            float score = hit.getScore();
            System.out.println("===================================================");
            String source = hit.getSourceAsString();
            System.out.println("version: " + version);
            System.out.println("id: " + id);
            System.out.println("index: " + index);
            System.out.println("type: " + type);
            System.out.println("score: " + score);
            System.out.println("source: " + source);
        }
    }

    @After
    public void cleanUp() {
        client.close();
    }
}
