package com.bfd.jubiter.stormkafka;

import java.io.IOException;
import java.util.Properties;

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
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.bfd.jubiter.hbase.service.JsonDataHandleService;

public class KafkaHbaseTopology {

	public static class HbasePutter extends BaseBasicBolt {
		private static final Log LOG = LogFactory.getLog(HbasePutter.class);

		// private static final String url =
		// "jdbc:mysql://localhost:3306/test?useUnicode=false&characterEncoding=UTF-8&useOldAliasMetadataBehavior=true";

		public void cleanup() {
			// TODO Auto-generated method stub

			LOG.info("~~~~~~~~~~~~~~~~~~ hbase over~~~~~~~~~~~~~~~~");
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				String msg = input.getString(0);
				JsonDataHandleService.addMessage(msg);
				LOG.info(msg + "-------------------");
			} catch (Exception e) {
				LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub

		}
	}

//	public static void main(String[] args) {
//		System.out.println(Thread.currentThread().getContextClassLoader()
//				.getResource(""));
//		System.out.println(KafkaHbaseTopology.class.getClassLoader()
//				.getResource(""));
//		System.out.println(ClassLoader.getSystemResource(""));
//		System.out.println(KafkaHbaseTopology.class.getResource(""));
//		System.out.println(KafkaHbaseTopology.class.getResource("/")); // Class文件所在路径
//	}

	public static void main(String[] args) {

		Properties properties = new Properties();
		try {
			properties.load(ClassLoader
					.getSystemResourceAsStream("consumer.properties"));
		} catch (IOException e1) {
			HbasePutter.LOG.error("(consumer.properties not found)配置文件异常");
			return;
		}

		String zks = properties.getProperty("zks"); // kafka's zk conenction
		String zkpath = properties.getProperty("zkpath");
		String topic = properties.getProperty("topic");
		String zkRoot = properties.getProperty("zkRoot"); // default zookeeper
															// root
															// configuration for
															// storm
		String id = properties.getProperty("id"); // consumer group id

		BrokerHosts brokerHosts = new ZkHosts(zks, zkpath);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = true;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf));
		builder.setBolt("hbase-putter", new HbasePutter()).shuffleGrouping(
				"kafka-reader");
		Config conf = new Config();
		conf.setDebug(true);
		String name = KafkaHbaseTopology.class.getSimpleName();

		if (args.length == 0) {
			// if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			// conf.put(Config.NIMBUS_HOST, args[0]);
			try {
				StormSubmitter.submitTopologyWithProgressBar(name, conf,
						builder.createTopology());

			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			try {
				Thread.sleep(6000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// cluster.shutdown();
		}
	}

}
