package alien4cloud.aspide;

import alien4cloud.paas.IPaaSCallback;
import alien4cloud.paas.model.AbstractMonitorEvent;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class EventService {

    TreeMap<Long, List<AbstractMonitorEvent>> events = new TreeMap<>();

    public void getEventsSince(Date date, int maxEvents, IPaaSCallback<AbstractMonitorEvent[]> eventCallback){
        SortedMap<Long, List<AbstractMonitorEvent>> longListSortedMap = events.tailMap(date.getTime());
        if(longListSortedMap != null) {
            List<AbstractMonitorEvent> result = new ArrayList<>();

            longListSortedMap.values().forEach(result::addAll);
            eventCallback.onSuccess(result.toArray(new AbstractMonitorEvent[]{}));
        }

    }

    public void registerEvent(Date date, AbstractMonitorEvent ev){
        registerEvent(date.getTime(), ev);
    }

    public void registerEvent(long date, AbstractMonitorEvent ev){
        events.putIfAbsent(date, new ArrayList<>());
        events.get(date).add(ev);
    }
}
