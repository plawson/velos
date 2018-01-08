package velos;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CityStatsBolt extends BaseWindowedBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    public void execute(TupleWindow inputWindow) {
        HashMap<String, HashMap<Long, ArrayList<Long>>> stationAvailableStands;
        stationAvailableStands = new HashMap<>();

        // Collect stats for all tuples, city by city, station by station
        Integer tupleCount = 0;
        for (Tuple input : inputWindow.get()) {
            String city = input.getStringByField("city");
            Long stationId = input.getLongByField("station_id");
            Long availableBikeStands = input.getLongByField("available_stands");

            stationAvailableStands.putIfAbsent(city, new HashMap<>());
            stationAvailableStands.get(city).putIfAbsent(stationId, new ArrayList<>());
            stationAvailableStands.get(city).get(stationId).add(availableBikeStands);

            this.outputCollector.ack(input);
            tupleCount += 1;
        }
        System.out.printf("====== CityStatsBolt: Received %d tuples\n", tupleCount);

        // Emit average stats, city by city
        String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        for(Entry<String, HashMap<Long, ArrayList<Long>>> cityStats : stationAvailableStands.entrySet()) {
            String city = cityStats.getKey();
            Double totalAvailableStands = 0.;
            for (Entry<Long, ArrayList<Long>> station: cityStats.getValue().entrySet()) {
                Double averageAvailableStands = 0.;
                for (Long availableStands: station.getValue()) {
                    averageAvailableStands += availableStands;
                }
                if (!station.getValue().isEmpty()) {
                    averageAvailableStands /= station.getValue().size();
                }
                totalAvailableStands += averageAvailableStands;
            }
            this.outputCollector.emit(new Values(city, now, totalAvailableStands));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("city", "date", "available_stands"));
    }
}
