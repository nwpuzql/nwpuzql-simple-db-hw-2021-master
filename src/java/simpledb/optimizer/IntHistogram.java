package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 * 选择性（selectivity），即查询条件匹配的记录占总记录数的比例
 */
public class IntHistogram {

    private int bucketNum, min, max, sum;
    private int[] buckets;
    private double bucketSize;   // 每个桶的宽度

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.min = min;
        this.max = max;
        this.sum = 0;
        if (max - min + 1 < buckets) {  // 桶的数量大于区间范围，设置桶的数量为范围大小，即一个桶一个数
            this.bucketNum = max - min + 1;
        } else {
            this.bucketNum = buckets;
        }
        this.bucketSize = (double) (max - min + 1) / bucketNum;
        this.buckets = new int[bucketNum];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index;
        index = (int) ((v - min) / bucketSize);  // 计算所在桶的索引
        sum++;
        buckets[index]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 选择性（selectivity），即查询条件匹配的记录占总记录数的比例
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        if (buckets.length == 0) {
            return 0.0;
        }
        int index = (int) ((v - min) / bucketSize);  // 计算所在桶的索引
        double leftBound, rightBound, granterThanSum, lessThanSum;
        switch (op) {
            case EQUALS:
                if (index >= buckets.length || index < 0) {
                    return 0.0;
                }
                return buckets[index] / bucketSize / sum;
            case NOT_EQUALS:
                if (index >= buckets.length || index < 0) {
                    return 1.0;
                }
                return 1 - (buckets[index] / bucketSize / sum);
            case GREATER_THAN:
                if (index >= buckets.length) {
                    return 0.0;
                }
                if (index < 0) {
                    return 1.0;
                }
                rightBound = min + (index + 1) * bucketSize - 1;
                granterThanSum = ((rightBound - v) / bucketSize) * buckets[index];
                for (int i = index + 1; i < buckets.length; i++) {  // 当前桶的右边区间总和
                    granterThanSum += buckets[i];
                }
                return granterThanSum / sum;
            case GREATER_THAN_OR_EQ:
                if (index >= buckets.length) {
                    return 0.0;
                }
                if (index < 0) {
                    return 1.0;
                }
                rightBound = min + (index + 1) * bucketSize - 1;
                granterThanSum = ((rightBound - v + 1) / bucketSize) * buckets[index];
                for (int i = index + 1; i < buckets.length; i++) {  // 当前桶的右边区间总和
                    granterThanSum += buckets[i];
                }
                return granterThanSum / sum;
            case LESS_THAN:
                if (index >= buckets.length) {
                    return 1.0;
                }
                if (index < 0) {
                    return 0.0;
                }
                leftBound = min + index * bucketSize;
                lessThanSum = ((leftBound - v) / bucketSize) * buckets[index];
                for (int i = index - 1; i >= 0; i--) {  // 当前桶的左边区间总和
                    lessThanSum += buckets[i];
                }
                return lessThanSum / sum;
            case LESS_THAN_OR_EQ:
                if (index >= buckets.length) {
                    return 1.0;
                }
                if (index < 0) {
                    return 0.0;
                }
                leftBound = min + index * bucketSize;
                lessThanSum = ((leftBound - v + 1) / bucketSize) * buckets[index];
                for (int i = index - 1; i >= 0; i--) {  // 当前桶的左边区间总和
                    lessThanSum += buckets[i];
                }
                return lessThanSum / sum;
            default:
                throw new IllegalArgumentException("Unsupported op: " + op);
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("IntHistogram%n"));
        sb.append(String.format("Buckets: %d, Min: %d, Max: %d, Bucket Size: %.2f%n", bucketNum, min, max, bucketSize));
        sb.append("Histogram Distribution:%n");

        for (int i = 0; i < bucketNum; i++) {
            double bucketMinValue = min + i * bucketSize;
            double bucketMaxValue = bucketMinValue + bucketSize - 1;
            sb.append(String.format("Bucket %d (%.2f - %.2f): ", i, bucketMinValue, bucketMaxValue));
            for (int j = 0; j < buckets[i]; j++) {
                sb.append("*");
            }
            sb.append(String.format(" (%d)%n", buckets[i]));
        }

        return sb.toString();
    }
}
