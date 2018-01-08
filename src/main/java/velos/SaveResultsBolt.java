package velos;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class SaveResultsBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            process(input);
            this.outputCollector.ack(input);
        } catch (IOException e) {
            e.printStackTrace();
            this.outputCollector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void process(Tuple input) throws IOException {
        String city = input.getStringByField("city");
        String date = input.getStringByField("date");
        Double availableStands = input.getDoubleByField("available_stands");
        String filePath = String.format("/home/plawson/kafka/kafka-storm/output/%s.csv", city);

        // check if file exists
        File csvFile = new File(filePath);
        if (!csvFile.exists()) {
            FileWriter fileWriter = new FileWriter(filePath);
            fileWriter.write("date;available stands\n");
            fileWriter.close();
        }

        // Write stats to file
        FileWriter fileWriter = new FileWriter(filePath, true);
        System.out.printf("====== SaveResultsBolt: %s %s - %f available stands\n", date, city, availableStands);
        fileWriter.write(String.format("%s;%f\n", date, availableStands));
        fileWriter.close();
    }
}
