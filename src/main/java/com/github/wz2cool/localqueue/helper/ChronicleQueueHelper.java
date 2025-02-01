package com.github.wz2cool.localqueue.helper;

import com.github.wz2cool.localqueue.model.enums.RollCycleType;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.DefaultCycleCalculator;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;

/**
 * Chronicle Queue Helper
 *
 * @author frank
 */
public class ChronicleQueueHelper {

    public static int cycle(final RollCycle rollCycle, final long timestamp) {
        long offsetMillis = System.currentTimeMillis() - timestamp;
        return DefaultCycleCalculator.INSTANCE.currentCycle(rollCycle, SystemTimeProvider.INSTANCE, offsetMillis);
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
}
