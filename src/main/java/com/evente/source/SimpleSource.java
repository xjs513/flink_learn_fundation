package com.evente.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSource implements SourceFunction<Long> {
    private boolean isRunning = true;
    private java.lang.Long index = 0L;
    private int t;

    public SimpleSource(int t){
        this.t = t;
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        long start = System.currentTimeMillis();
        long now ;
        while (isRunning){
            now = System.currentTimeMillis();
            if (now - start > t)
                cancel();
            sourceContext.collect(index);
            index ++;
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
