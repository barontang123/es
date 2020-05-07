package es.com;

import com.alibaba.fastjson.JSON;
import com.sun.org.apache.xerces.internal.impl.PropertyManager;
import javafx.scene.NodeBuilder;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

public class testClient {

    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );

       // new testClient().addIndex1();
        //new testClient().getResult();
       // new testClient().addIndex2();
       //  new testClient().getData1(); //获取单条数据
       // new testClient().testBulkIndex();  //批量建索引
       // new testClient().searchAll();   //搜索
        // new testClient().deleteIndex();  //删除索引

        // new testClient().searchInclude();//数据列过滤查询，指定只查询几列数据；
         new testClient().searchByCondition();  //简单条件查询
        // new testClient().searchHighlight();  //条件查询高亮实现
        // new testClient().searchMutil();  // must 条件查询
        //new testClient().searchMutilMustNot();  //多条件查询  mustNot

       // new testClient().searchMutil3();  // should 条件查询
        // new testClient().searchMutilRange(); // range 条件查询

        //new testClient().search2();

        int i = 0;

    }

    private static final String ANALYZER="smartcn";

    public void search2()  throws Exception {
                  client = getClient();
                  AnalyzeRequest analyzeRequest = new AnalyzeRequest("user")
                        .text("张三李四")
                        .analyzer("ik_max_word");

            List<AnalyzeResponse.AnalyzeToken> tokens = client.admin().indices()
                    .analyze(analyzeRequest)
                    .actionGet()
                    .getTokens();

            for (AnalyzeResponse.AnalyzeToken token : tokens) {
                System.out.println(token.getTerm());
            }
            closeClient();
         }

    public void addIndex1() throws  Exception {
        client = getClient();
        IndexResponse response = client.prepareIndex("msg", "tweet", "1").setSource(XContentFactory.jsonBuilder()
                .startObject().field("userName", "张三")
                .field("sendDate", new Date())
                .field("msg", "你好李四")
                .endObject()).get();

        closeClient();
      /*  System.console().printf("索引名称:" + response.getIndex() + "\n类型:" + response.getType()
                + "\n文档ID:" + response.getId() + "\n当前实例状态:" + response.status());*/
    }

    /**
     * 删除索引
     * @throws Exception
     */
    public void deleteIndex() throws Exception
    {
        client = getClient();
        Object object = client.admin().indices().prepareDelete("user").execute().actionGet();
        closeClient();

        /*
        //删除索引twitter
        client.admin().indices().prepareDelete("twitter").execute().actionGet();
        //删除索引中的某个文档（一条数据）
        client.prepareDelete("twitter", "tweet", "1");*/
    }

    /**
     * 批量建索引
     * @throws Exception
     */
    public void testBulkIndex() throws Exception {
        client = getClient();
        List<UserInfo> userInfoList = new ArrayList<>();
        List<IndexRequest> requests = new ArrayList<IndexRequest>();
        for (int i = 1; i < 10; i++) {
            UserInfo userInfo = new UserInfo();

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date();
            String dateStr = simpleDateFormat.format(date);

            userInfo.setSendDate(new Date().getTime());
            userInfo.setMsg(String.valueOf(i));
            userInfo.setUserName("张三" + i);
            userInfo.setId(i);
            userInfoList.add(userInfo);

            String index = "user"; // 相当于数据库名
            String type = "userInfo"; // 相当于表名

            String json = JSON.toJSONString(userInfo);

            IndexRequest request = client
                    .prepareIndex(index, type, String.valueOf(userInfo.getId())).setSource(json, XContentType.JSON)
                    .request();

            requests.add(request);
        }

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (IndexRequest request : requests) {
            bulkRequest.add(request);
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.out.print("生成索引失败！");
        }
        closeClient();
    }


    /**
     * 数据列过滤查询
     * @throws Exception
     */
      public void searchInclude() throws Exception  {
             client = getClient();
             SearchRequestBuilder srb=client.prepareSearch("user").setTypes("userInfo");
             SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
                     .setFetchSource(new String[] {"id","userName"},null)
                     .execute()
                     .actionGet();
             SearchHits hits=sr.getHits();
             for (SearchHit hit : hits) {
                 System.out.println(hit.getSourceAsString());
             }

            closeClient();
        }

    /**
     * 简单条件查询
     */
    public void searchByCondition() throws Exception{
        client = getClient();
         SearchRequestBuilder srb=client.prepareSearch("user").setTypes("userInfo");
         SearchResponse sr=srb.setQuery(QueryBuilders.matchQuery("userName","张飞李四旺达"))
                 .setFetchSource(new String[] {"id","userName"},null)
                 .execute()
                 .actionGet();
         SearchHits hits=sr.getHits();
         for (SearchHit hit : hits) {
                 System.out.println(hit.getSourceAsString());
             }
         closeClient();
    }

   /**
   * 条件查询高亮实现
   */
    public void searchHighlight() throws Exception {
        client = getClient();
        SearchRequestBuilder srb=client.prepareSearch("user").setTypes("userInfo");
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        highlightBuilder.field("userName");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchQuery("userName","2"))
                .highlighter(highlightBuilder)
                .setFetchSource(new String[] {"id","userName"},null)
                .execute()
                .actionGet();
        SearchHits hits=sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getHighlightFields());
            Map<String, HighlightField> map = hit.getHighlightFields();

            for(Map.Entry entry : map.entrySet()){
                String mapKey = entry.getKey().toString();
                HighlightField mapValue =(HighlightField)entry.getValue();

                for (Text text:mapValue.getFragments())
                {
                    System.out.println(mapKey+":"+text.toString());
                }
            }
        }
        closeClient();
    }

       /**
       * 多条件查询  must
       */
     public void searchMutil() throws Exception {
         client = getClient();
         SearchRequestBuilder srb =client.prepareSearch("user").setTypes("userInfo");
         QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("userName", "张三");
         QueryBuilder queryBuilder2=QueryBuilders.matchPhraseQuery("id", "3");
         SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                         .must(queryBuilder)
                         .must(queryBuilder2)
                       )
                 .execute()
                 .actionGet();
         SearchHits hits=sr.getHits();
         for (SearchHit hit : hits) {
                 System.out.println(hit.getSourceAsString());
         }
         closeClient();
     }

     /**
     * 多条件查询  mustNot
     */
     public void searchMutilMustNot() throws Exception {
         client = getClient();
         SearchRequestBuilder srb =client.prepareSearch("user").setTypes("userInfo");
         QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("userName", "张三");
         QueryBuilder queryBuilder2=QueryBuilders.matchPhraseQuery("userName", "1");
         SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                         .must(queryBuilder)
                         .mustNot(queryBuilder2))
                 .execute()
                 .actionGet();
         SearchHits hits=sr.getHits();
         for (SearchHit hit : hits) {
                 System.out.println(hit.getSourceAsString());
             }
         closeClient();
     }

      /**
        * 多条件查询  should提高得分
        * @throws Exception
        */
     public void searchMutil3()throws Exception{
      client = getClient();
     SearchRequestBuilder srb=client.prepareSearch("user").setTypes("userInfo");
     QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("userName", "张三");
     QueryBuilder queryBuilder2=QueryBuilders.matchPhraseQuery("userName", "1");
     QueryBuilder queryBuilder3=QueryBuilders.rangeQuery("id").lte("3");
     SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                     .must(queryBuilder)
                     .should(queryBuilder2)
                     .must(queryBuilder3)
                )
         .execute()
         .actionGet();
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
             System.out.println(hit.getScore()+":"+hit.getSourceAsString());
         }

         closeClient();
     }

    /***
    * 多条件查询 range限制范围
    */
     public void searchMutilRange() throws Exception {

         client = getClient();
         SearchRequestBuilder srb = client.prepareSearch("user").setTypes("userInfo");
         QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("userName", "张三");
         QueryBuilder queryBuilder2=QueryBuilders.rangeQuery("id").gt(4);
         SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                         .must(queryBuilder)
                         .filter(queryBuilder2)).addSort("id",SortOrder.ASC)
                 .setFrom(0).setSize(2)
                 .execute()
                 .actionGet();
         SearchHits hits=sr.getHits();
         for (SearchHit hit : hits) {
                 System.out.println(hit.getSourceAsString());
             }
        closeClient();
     }

     public void searchAll() throws Exception {
        client = getClient();
                 SearchRequestBuilder srb=client.prepareSearch("user").setTypes("userInfo");
                 //SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();//查询所有
                 SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
                         .addSort("sendDate",SortOrder.ASC).setFrom(0).setSize(4).execute().actionGet();
                 SearchHits hits=sr.getHits();
                 for(SearchHit hit:hits) {
                         System.out.println(hit.getSourceAsString());
                     }
        closeClient();
    }

    public void addIndex2() throws  Exception {

        client = getClient();
        UserInfo userInfo = new UserInfo();
        userInfo.setMsg("李四厉害");
        userInfo.setUserName("李四");
        userInfo.setSendDate(new Date().getTime());
        String jsonStr = JSON.toJSONString(userInfo);
        IndexResponse response = client.prepareIndex("weixin", "tweet").setSource(jsonStr, XContentType.JSON).get();
        closeClient();
        System.out.println( "成功");
    }

    public void getData1() throws  Exception {
        client = getClient();
        GetResponse getResponse = client.prepareGet("user", "userInfo", "1").get();
        closeClient();
     }

    private  TransportClient client = null;
    public  void getResult() throws Exception{
        client = getClient();
        SearchResponse response = client.prepareSearch("blog","index")//创建查询索引,参数productindex表示要查询的索引库为blog、index
                .setTypes("article")  //设置type
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)//设置查询类型 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询  2.SearchType.SCAN =扫描查询,无序
                .setQuery(QueryBuilders.termQuery("content", "today"))  //设置查询项以及对应的值
                //	        .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // 设置Filter过滤
                .setFrom(0).setSize(60)//设置分页
                .setExplain(true) //设置是否按查询匹配度排序
                //        .addSort("id", SortOrder.DESC)//设置按照id排序
                .execute()
                .actionGet();
        SearchHits hits = response.getHits();
        System.out.println("总数："+hits.getTotalHits());
        for(SearchHit hit: hits.getHits()) {
            if(hit.getSourceAsMap().containsKey("title")) {
                System.out.println("source.title: " + hit.getSourceAsMap().get("title"));
            }
        }
        System.out.println(response.toString());
        closeClient();
    }




    public   TransportClient getClient() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name", "my-application").build();
        client =  new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.1.2.35"), 9300));
        return client;
    }


    public  void closeClient(){
        if (this.client != null){
            this.client.close();
        }
    }

}
