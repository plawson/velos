package velos;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException,
            AuthorizationException
    {
        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder;
        spoutConfigBuilder =
                KafkaSpoutConfig.builder("localhost:9092,localhost:9093", "velib-stations");
        spoutConfigBuilder.setGroupId("city-stats");
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
        builder.setSpout("stations", new KafkaSpout<>(spoutConfig), 10);

        builder.setBolt("station-parsing", new StationParsingBolt(), 7)
                .shuffleGrouping("stations");

        builder.setBolt("city-stats", new CityStatsBolt()
                .withTumblingWindow(BaseWindowedBolt.Duration.of(1000*60*5)), 7)
                .fieldsGrouping("station-parsing", new Fields("city"));

        builder.setBolt("save-results", new SaveResultsBolt(), 7)
                .fieldsGrouping("city-stats", new Fields("city"));

        StormTopology topology = builder.createTopology();

        Config config = new Config();
        config.setMessageTimeoutSecs(60*30);
        config.setNumWorkers(4); // 4 Worker processes
        String topologyName = "velos";
        if (args.length > 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology(topologyName, config, topology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, topology);
        }
    }
}
