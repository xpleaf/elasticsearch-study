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
 * 分页
 * 高亮显示
 */
public class ElasticSearchTest2 {

    private TransportClient client;
    private String index = "bigdata";
    private String type = "product";
    private String[] indics = {"bigdata", "bank"};

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
     * 1.精确查询
     * termQuery
     * term就是一个字段
     */
    @Test
    public void testSearch1() {
        SearchRequestBuilder searchQuery = client.prepareSearch(indics)    // 在prepareSearch()的参数为索引库列表，意为要从哪些索引库中进行查询
                .setSearchType(SearchType.DEFAULT)  // 设置查询类型，有QUERY_AND_FETCH  QUERY_THEN_FETCH  DFS_QUERY_AND_FETCH  DFS_QUERY_THEN_FETCH
                .setQuery(QueryBuilders.termQuery("author", "apache"))// 设置相应的query，用于检索，termQuery的参数说明：name是doc中的具体的field，value就是要找的具体的值
                ;
        // 如果上面不加查询条件，则会查询所有
        SearchResponse response = searchQuery.get();

        showResult(response);
    }

    /**
     * 2.模糊查询
     * prefixQuery
     */
    @Test
    public void testSearch2() {
        SearchResponse response = client.prepareSearch(indics).setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.prefixQuery("name", "h"))
                .get();
        showResult(response);
    }

    /**
     * 3.分页查询
     * 查询索引库bank中
     * 年龄在(25, 35]之间的数据信息
     *
     * 分页算法：
     *      查询的第几页，每一页显示几条
     *          每页显示10条记录
     *
     *      查询第4页的内容
     *          setFrom(30=(4-1)*size)
     *          setSize(10)
     *       所以第N页的起始位置：(N - 1) * pageSize
     */
    @Test
    public void testSearch3() {
        // 注意QUERY_THEN_FETCH和注意QUERY_AND_FETCH返回的记录数不一样，前者默认10条，后者是50条（5个分片）
        SearchResponse response = client.prepareSearch(indics).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.rangeQuery("age").gt(25).lte(35))
                // 下面setFrom和setSize用于设置查询结果进行分页
                .setFrom(0)
                .setSize(5)
                .get();
        showResult(response);
    }

    /**
     * 4.高亮显示查询
     * 获取数据，
     *  查询apache，不仅在author拥有，也可以在url，在name中也可能拥有
     *  author or url   --->booleanQuery中的should操作
     *      如果是and的类型--->booleanQuery中的must操作
     *      如果是not的类型--->booleanQuery中的mustNot操作
     *  使用的match操作，其实就是使用要查询的keyword和对应字段进行完整匹配，是否相等，相等返回
     */
    @Test
    public void testSearch4() {
        SearchResponse response = client.prepareSearch(indics).setSearchType(SearchType.DEFAULT)
//                .setQuery(QueryBuilders.multiMatchQuery("apache", "author", "url"))
//                .setQuery(QueryBuilders.regexpQuery("url", ".*apache.*"))
//                .setQuery(QueryBuilders.termQuery("author", "apache"))
                .setQuery(QueryBuilders.boolQuery()
                        .should(QueryBuilders.regexpQuery("url", ".*apache.*"))
                        .should(QueryBuilders.termQuery("author", "apache")))
                // 设置高亮显示--->设置相应的前置标签和后置标签
                .setHighlighterPreTags("<span color='blue' size='18px'>")
                .setHighlighterPostTags("</span>")
                // 哪个字段要求高亮显示
                .addHighlightedField("author")
                .addHighlightedField("url")
                .get();
        SearchHits searchHits = response.getHits();
        float maxScore = searchHits.getMaxScore();  // 查询结果中的最大文档得分
        System.out.println("maxScore: " + maxScore);
        long totalHits = searchHits.getTotalHits(); // 查询结果记录条数
        System.out.println("totalHits: " + totalHits);
        SearchHit[] hits = searchHits.getHits();    // 查询结果
        System.out.println("当前返回结果记录条数：" + hits.length);
        for(SearchHit hit : hits) {
            System.out.println("========================================================");
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            for(Map.Entry<String , HighlightField> me : highlightFields.entrySet()) {
                System.out.println("--------------------------------------");
                String key = me.getKey();
                HighlightField highlightField = me.getValue();
                String name = highlightField.getName();
                System.out.println("key: " + key + ", name: " + name);
                Text[] texts = highlightField.fragments();
                String value = "";
                for(Text text : texts) {
                    // System.out.println("text: " + text.toString());
                    value += text.toString();
                }
                System.out.println("value: " + value);
            }
        }
    }

    /**
     * 5.排序查询
     * 对结果集进行排序
     *  balance（收入）由高到低
     */
    @Test
    public void testSearch5() {
        // 注意QUERY_THEN_FETCH和注意QUERY_AND_FETCH返回的记录数不一样，前者默认10条，后者是50条（5个分片）
        SearchResponse response = client.prepareSearch(indics).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.rangeQuery("age").gt(25).lte(35))
                .addSort("balance", SortOrder.DESC)
                // 下面setFrom和setSize用于设置查询结果进行分页
                .setFrom(0)
                .setSize(5)
                .get();
        showResult(response);
    }

    /**
     * 6.聚合查询：计算平均值
     */
    @Test
    public void testSearch6() {
        indics = new String[]{"bank"};
        // 注意QUERY_THEN_FETCH和注意QUERY_AND_FETCH返回的记录数不一样，前者默认10条，后者是50条（5个分片）
        SearchResponse response = client.prepareSearch(indics).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.rangeQuery("age").gt(25).lte(35))
                /*
                    select avg(age) as avg_name from person;
                    那么这里的avg("balance")--->就是返回结果avg_name这个别名
                 */
                .addAggregation(AggregationBuilders.avg("avg_balance").field("balance"))
                .addAggregation(AggregationBuilders.max("max").field("balance"))
                .get();
//        System.out.println(response);
        /*
            response中包含的Aggregations
                "aggregations" : {
                    "max" : {
                      "value" : 49741.0
                    },
                    "avg_balance" : {
                      "value" : 25142.137373737372
                    }
                  }
                  则一个aggregation为：
                  {
                      "value" : 49741.0
                    }
         */
        Aggregations aggregations = response.getAggregations();
        List<Aggregation> aggregationList = aggregations.asList();
        for(Aggregation aggregation : aggregationList) {
            System.out.println("========================================");
            String name = aggregation.getName();
            // Map<String, Object> map = aggregation.getMetaData();
            System.out.println("name: " + name);
            // System.out.println(map);
            Object obj = aggregation.getProperty("value");
            System.out.println(obj);
        }
        /*Aggregation avgBalance = aggregations.get("avg_balance");
        Object obj = avgBalance.getProperty("value");
        System.out.println(obj);*/
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
