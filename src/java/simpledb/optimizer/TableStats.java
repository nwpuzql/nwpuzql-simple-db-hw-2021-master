package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    private IntHistogram stats;
    private int tableId;
    private int ioCostPerPage = IOCOSTPERPAGE;
    private int sumCost;
    private int minFieldValue[];
    private int maxFieldValue[];
    private int tupleNum;
    private int pageNum;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 构造时对所有元组进行第一遍扫描，记录下所有int字段的最大和最小值
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.sumCost = 0;
        this.tupleNum = 0;
        HeapFile dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.pageNum = dbFile.numPages();
        TupleDesc tupleDesc = dbFile.getTupleDesc();
        int fieldNum = tupleDesc.numFields();
        this.maxFieldValue = new int[fieldNum];
        this.minFieldValue = new int[fieldNum];
        Arrays.fill(maxFieldValue, Integer.MIN_VALUE);
        Arrays.fill(minFieldValue, Integer.MAX_VALUE);
        DbFileIterator dbIt = dbFile.iterator(new TransactionId());
        try {
            dbIt.open();
            while (dbIt.hasNext()) {  // 遍历tuple找出每个字段的最大值和最小值
                Tuple t = dbIt.next();
                this.tupleNum++;
                for (int i = 0; i < fieldNum; i++) {
                    Field f = t.getField(i);
                    if (f.getType() == Type.INT_TYPE) {
                        IntField intF = (IntField) f;
                        if (intF.getValue() > maxFieldValue[i])
                            maxFieldValue[i] = intF.getValue();
                        if (intF.getValue() < minFieldValue[i])
                            minFieldValue[i] = intF.getValue();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.pageNum * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 将选择概率与元组总数相乘
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified,筛选之后的元组估计个数
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.round(tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 通过遍历tuple，将每个tuple中field索引的字段值加入hist，并返回选择性的值
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        HeapFile dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableId);
        TupleDesc td = dbFile.getTupleDesc();
        double selectivity = 0.0;
        if (td.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram hist = new IntHistogram(NUM_HIST_BINS, minFieldValue[field], maxFieldValue[field]);
            DbFileIterator dbIt = dbFile.iterator(new TransactionId());
            try {
                dbIt.open();
                while (dbIt.hasNext()) {
                    Tuple t = dbIt.next();
                    IntField f = (IntField) t.getField(field);
                    hist.addValue(f.getValue());
                }
                selectivity = hist.estimateSelectivity(op, ((IntField) constant).getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            StringHistogram hist = new StringHistogram(NUM_HIST_BINS);
            DbFileIterator dbIt = dbFile.iterator(new TransactionId());
            try {
                dbIt.open();
                while (dbIt.hasNext()) {
                    Tuple t = dbIt.next();
                    StringField f = (StringField) t.getField(field);
                    hist.addValue(f.getValue());
                }
                selectivity = hist.estimateSelectivity(op, ((StringField) constant).getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return this.tupleNum;
    }

}
