package qwj.elasticsearch.esdemo.configuration;

import cn.hutool.log.StaticLog;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author qwj
 * @Date 2021/3/12 9:55
 * @Description 创建ES client 实例
 */
@Configuration
public class ESClientInstance {
    /**
     * 返回低级客户端
     * @return
     */
    @Bean
    public RestClient getLowClient(){
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("127.0.0.1", 9200, "http")
        );
        //RestClientBuilder 还允许在构建RestClient 实例时有选择地设置以下配置参数
        //通常用于认证
        Header[] headers = new Header[]{new BasicHeader("header", "value")};
        restClientBuilder.setDefaultHeaders(headers);  //设置每个请求需要发送地默认标头,以避免必须为每个请求指定他们
        
        //设置一个侦听器,该侦听器在每次节点发生故障时得到通知,以防需要采取操作.启用故障嗅探
        //时在内部使用
        restClientBuilder.setFailureListener(new RestClient.FailureListener(){
           @Override
           public void onFailure(Node node) {
               String name = node.getName();
               int port = node.getHost().getPort();
               String hostAddress = node.getHost().getAddress().getHostAddress();
               StaticLog.info("节点{}(端口:{},地址:{})出现故障,请及时处理!!",name,port,hostAddress);
           } 
        });

        //过滤掉master，data，Ingest节点
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);



        RestClient client = restClientBuilder.build();
        return client;
    }
}
