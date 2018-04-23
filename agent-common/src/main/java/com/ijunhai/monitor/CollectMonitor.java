package com.ijunhai.monitor;

import com.ijunhai.emitter.Emitter;
import com.ijunhai.emitter.LogEmitter;
import com.ijunhai.metric.CollectEvent;
import com.ijunhai.metric.CollectMetrics;
import com.ijunhai.metric.MetricType;
import com.ijunhai.util.PropertiesUtils;

import static com.ijunhai.metric.CollectEvent.HOST;

public class CollectMonitor extends AbstractMonitor {

    private CollectMetrics previousCollectMetrics;
    private CollectEvent.Builder builder;

    private final CollectMetrics collectMetrics;

    public CollectMonitor(String host, String topic, String fileName, MetricType metricType, CollectMetrics collectMetrics) {
        this.previousCollectMetrics = new CollectMetrics();
        this.builder = CollectEvent.builder(host, topic,fileName, metricType.name());
        this.collectMetrics = collectMetrics;
    }

    public CollectMetrics getCollectMetrics() {
        return collectMetrics;
    }

    @Override
    public boolean doMonitor(Emitter emitter) {
        CollectMetrics snapshot = collectMetrics.snapshot();
        emitter.emit(builder.build("logReaded", snapshot.logReaded() - previousCollectMetrics.logReaded()));
        emitter.emit(builder.build("sended", snapshot.sended() - previousCollectMetrics.sended()));
        previousCollectMetrics = snapshot;
        return true;
    }

}
