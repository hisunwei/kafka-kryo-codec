package kafkastorm.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.storm.HostPort;
import kafka.storm.KafkaConfig.StaticHosts;


public class StaticBrokerReader implements IBrokerReader {

    Map<String, List> brokers = new HashMap();
    
    public StaticBrokerReader(StaticHosts hosts) {
        for(HostPort hp: hosts.hosts) {
            List info = new ArrayList();
            info.add((long) hp.port);
            info.add((long) hosts.partitionsPerHost);
            brokers.put(hp.host, info);
        }
    }
    
    @Override
    public Map<String, List> getCurrentBrokers() {
        return brokers;
    }

    @Override
    public void close() {
    }
}
