package com.github.wz2cool.localqueue.helper;

import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.DefaultCycleCalculator;
import net.openhft.chronicle.queue.RollCycle;

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
}
