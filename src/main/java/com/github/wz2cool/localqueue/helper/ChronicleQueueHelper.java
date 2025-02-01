package com.github.wz2cool.localqueue.helper;

import com.github.wz2cool.localqueue.model.enums.RollCycleType;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.DefaultCycleCalculator;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;

import java.util.TimeZone;

/**
 * Chronicle Queue Helper
 *
 * @author frank
 */
public class ChronicleQueueHelper {

    public static int cycle(final RollCycle rollCycle, TimeProvider timeProvider, final long timestamp) {
        long offsetMillis = System.currentTimeMillis() - timestamp;
        return DefaultCycleCalculator.INSTANCE.currentCycle(rollCycle, timeProvider, offsetMillis);
    }

    public static RollCycle getRollCycle(final RollCycleType rollCycleType) {
        switch (rollCycleType) {
            case HOURLY:
                return RollCycles.FAST_HOURLY;
            case DAILY:
                return RollCycles.FAST_DAILY;
            default:
                throw new IllegalArgumentException("rollCycleType is not support.");
        }
    }

    public static TimeProvider getTimeProvider(TimeZone timeZone) {
        return () -> {
            long currentTime = System.currentTimeMillis();
            return currentTime + timeZone.getOffset(currentTime);
        };

    }
}
