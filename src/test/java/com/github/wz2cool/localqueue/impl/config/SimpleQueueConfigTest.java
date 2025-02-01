package com.github.wz2cool.localqueue.impl.config;

import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.enums.RollCycleType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("all")
public class SimpleQueueConfigTest {

    private SimpleQueueConfig.Builder builder;

    @BeforeEach
    public void setUp() {
        builder = new SimpleQueueConfig.Builder();
    }

    @Test
    public void build_DataDirIsNull_ThrowsIllegalArgumentException() {
        builder.setKeepDays(10)
                .setRollCycleType(RollCycleType.HOURLY)
                .setTimeZone(TimeZone.getDefault());

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void build_KeepDaysLessThanMinusOne_ThrowsIllegalArgumentException() {
        builder.setDataDir(new File("/tmp"))
                .setKeepDays(-2)
                .setRollCycleType(RollCycleType.HOURLY)
                .setTimeZone(TimeZone.getDefault());

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void build_RollCycleTypeIsNull_ThrowsIllegalArgumentException() {
        builder.setDataDir(new File("/tmp"))
                .setKeepDays(10)
                .setRollCycleType(null)
                .setTimeZone(TimeZone.getDefault());

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void build_TimeZoneIsNull_ThrowsIllegalArgumentException() {
        builder.setDataDir(new File("/tmp"))
                .setKeepDays(10)
                .setTimeZone(null)
                .setRollCycleType(RollCycleType.HOURLY);

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void build_ValidInputs_ReturnsSimpleQueueConfig() {
        File dataDir = new File("/tmp");
        int keepDays = 10;
        RollCycleType rollCycleType = RollCycleType.HOURLY;
        TimeZone timeZone = TimeZone.getDefault();

        SimpleQueueConfig config = builder.setDataDir(dataDir)
                .setKeepDays(keepDays)
                .setRollCycleType(rollCycleType)
                .setTimeZone(timeZone)
                .build();

        assertEquals(dataDir, config.getDataDir());
        assertEquals(keepDays, config.getKeepDays());
        assertEquals(rollCycleType, config.getRollCycleType());
        assertEquals(timeZone, config.getTimeZone());
    }
}
