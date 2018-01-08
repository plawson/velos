package velos;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class StationParsingBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        try {
            process(tuple);
        } catch (ParseException e) {
            e.printStackTrace();
            this.outputCollector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("city", "station_id", "available_stands"));
    }

    private void process(Tuple input) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
        String contract = (String)obj.get("contract_name");
        Long availableStands = (Long)obj.get("available_bike_stands");
        Long stationNumber = (Long)obj.get("number");

        this.outputCollector.emit(new Values(contract, stationNumber, availableStands));
        this.outputCollector.ack(input);
    }
}
