package com.bfd.jubiter.stormkafka;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;






import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyKafkaTopology {

     public static class KafkaWordSplitter extends BaseRichBolt {
        /**
		 * 
		 */
		private static final long serialVersionUID = 4553448672930779821L;
		private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);
          private OutputCollector collector;
          public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
               this.collector = collector;              
          }
          public void execute(Tuple input) {
               String line = input.getString(0);
               LOG.info("RECV[kafka -> splitter] " + line);
               String[] words = line.split("\\s+");
               for(String word : words) {
                    LOG.info("EMIT[splitter -> counter] " + word);
                    collector.emit(input, new Values(word, 1));
               }
               collector.ack(input);
          }
          public void declareOutputFields(OutputFieldsDeclarer declarer) {
               declarer.declare(new Fields("word", "count"));         
          }
     }
     public static class WordCounter extends BaseRichBolt {
          /**
		 * 
		 */
		private static final long serialVersionUID = -8558313812118295728L;
		  private static final Log LOG = LogFactory.getLog(WordCounter.class);
          public static final String url = "jdbc:mysql://localhost:3306/jupiterdatabase?useUnicode=false&characterEncoding=UTF-8&useOldAliasMetadataBehavior=true";  
          public static final String name = "com.mysql.jdbc.Driver";  
          public static final String user = "root";  
          public static final String password = "qianfendian";  
          public static Connection conn = null;
          public static PreparedStatement pst = null;
          private OutputCollector collector;
          private Map<String, AtomicInteger> counterMap;
          static{
        	  try {  
                  Class.forName(name);//指定连接类型  
                  conn = DriverManager.getConnection(url, user, password);//获取连接  
                  pst = conn.prepareStatement("insert into t_kafka_storm (word,count) values (?,?)");//准备执行语句  
              } catch (Exception e) {  
                  e.printStackTrace();  
              }  
          }
          public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector) {
               this.collector = collector;    
               this.counterMap = new HashMap<String, AtomicInteger>();
          }

          public void execute(Tuple input){
               String word = input.getString(0);
               int count = input.getInteger(1);
               LOG.info("RECV[splitter -> counter] " + word + " : " + count);
               AtomicInteger ai = this.counterMap.get(word);
               if(ai == null) {
                    ai = new AtomicInteger();
                    this.counterMap.put(word, ai);
               }
               ai.addAndGet(count);
               collector.ack(input);
               LOG.info("CHECK statistics map: " + this.counterMap);
				try{
					pst.setString(1, word);
					pst.setInt(2, ai.get());
					pst.execute();
				}catch(Exception e){
					throw new RuntimeException("数据库执行出错！"+e.getMessage());
				}
          }

          @Override
          public void cleanup() {
               LOG.info("The final result:");
               Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
               while(iter.hasNext()) {
                    Entry<String, AtomicInteger> entry = iter.next();
                    LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
               }
              
          }

          public void declareOutputFields(OutputFieldsDeclarer declarer) {
               declarer.declare(new Fields("word", "count"));         
          }
     }
    
     public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
          String zks = "service201:2182/kafka0.8.2.1";
          String topic = "kafka_storm_topic";
          String zkRoot = "/storm"; // default zookeeper root configuration for storm
          String id = "word";
          BrokerHosts brokerHosts = new ZkHosts(zks);
          SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
          spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
          spoutConf.forceFromStart = false;
          spoutConf.zkServers = Arrays.asList(new String[] {"service201"});
          spoutConf.zkPort = 2182;
          TopologyBuilder builder = new TopologyBuilder();
          builder.setSpout("kafka-reader", new KafkaSpout(spoutConf)); // Kafka我们创建了一个1分区的Topic，这里并行度设置为5
          builder.setBolt("word-splitter", new KafkaWordSplitter()).shuffleGrouping("kafka-reader");
          builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));
          Config conf = new Config();
          //配置Kafka broker地址 
          String name = MyKafkaTopology.class.getSimpleName();
          if (args != null && args.length > 0) {
               // Nimbus host name passed from command line
               conf.put(Config.NIMBUS_HOST, args[0]);
               conf.setNumWorkers(3);
               StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
          } else {
               conf.setMaxTaskParallelism(3);
               LocalCluster cluster = new LocalCluster();
               cluster.submitTopology(name, conf, builder.createTopology());
               Thread.sleep(60000);
               cluster.shutdown();
          }
     }
}