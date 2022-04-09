package com.skydawn.flink.runtime.state;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

/**
 * @author listening
 */
public class KeyGroupRangeAssignmentTest {

    private void computeKeyGroupRangeForOperatorIndexAlgorithm(int maxParallelism, int parallelism) {
        for (int operatorIndex = 0; operatorIndex < parallelism; operatorIndex++) {
            int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
            int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
            System.out.println("start: " + start + ", end: " + end);
        }
        System.out.println("****************");
    }

    @Test
    public void testComputeKeyGroupRangeForOperatorIndexAlgorithm() {
        int maxParallelism = 50;

        computeKeyGroupRangeForOperatorIndexAlgorithm(maxParallelism, 20);

        computeKeyGroupRangeForOperatorIndexAlgorithm(maxParallelism, 30);
    }

}
