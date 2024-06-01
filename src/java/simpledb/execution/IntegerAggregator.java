package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield = NO_GROUPING;
    private Type type;
    private int afield;
    private Op op;
    private Map<Field, Integer> aggregates;
    /* 使用cnt计算求平均会存在很大的误差
     * private Map<Field, Integer> cnt;  // 只在avg的时候使用，用以记录每组的个数
     * 改用前n个数的sum
     * todo 可能还存在sum超出int范围的问题
     */
    private Map<Field, Integer> cnt;  // 只在avg的时候使用，用以记录每组的个数
    private Map<Field, Integer> sum;  // 只在avg的时候使用，用以记录每组的之前的总和
    public TupleDesc td = null;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.type = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.aggregates = new HashMap<Field, Integer>();
        this.sum = new HashMap<Field, Integer>();
        this.cnt = new HashMap<Field, Integer>();
        if (gbfield == NO_GROUPING && type == null) {  // 没有group by子句，tuple和tupleDesc都只有一个字段
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{type, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbf = null;
        if (this.gbfield == NO_GROUPING && type == null) {
            gbf = new IntField(Integer.MIN_VALUE);  // 使用int最小值表示无gbf
        } else {
            gbf = tup.getField(this.gbfield);
        }
        Field af = tup.getField(this.afield);
        int newValue = ((IntField) af).getValue();
        if (!aggregates.containsKey(gbf)) {  // 第一个元素加入的情况
            if (this.op == Op.COUNT) {       // op==count的情况
                aggregates.put(gbf, 1);
            } else if (this.op == Op.AVG) {  // op==avg的情况
                aggregates.put(gbf, newValue);
                sum.put(gbf, newValue);
                cnt.put(gbf, 1);
            } else {                        // op是其他的情况
                aggregates.put(gbf, newValue);
            }
        } else {                            // 后面的元素加入的情况
            int aRes = aggregates.get(gbf);
            switch (this.op) {
                case MIN:
                    if (newValue < aRes) {
                        aggregates.put(gbf, newValue);
                    }
                    break;
                case MAX:
                    if (newValue > aRes) {
                        aggregates.put(gbf, newValue);
                    }
                    break;
                case SUM:
                    newValue += aRes;
                    aggregates.put(gbf, newValue);
                    break;
                case AVG:
                    int preSum = sum.get(gbf);        // 之前的总和
                    int n = cnt.get(gbf);             // 之前每组的个数
                    sum.put(gbf, preSum + newValue);  // 总和更新
                    cnt.put(gbf, n + 1);              // 更新组个数
                    newValue = (preSum + newValue) / (n + 1);
                    aggregates.put(gbf, newValue);    // 平均数更新
                    break;
                case COUNT:
                    newValue = aRes + 1;
                    aggregates.put(gbf, newValue);
                    break;
                default:
                    throw new IllegalStateException("unknown op: " + this.op);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {

        // some code goes here
        TupleDesc finalTd = td;
        return new OpIterator() {
            private Iterator<Map.Entry<Field, Integer>> it = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                // 在这里初始化迭代器
                it = aggregates.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (it == null) {
                    throw new IllegalStateException("Iterator not open");
                }

                // 检查是否还有更多的元素
                return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (it == null || !it.hasNext()) {
                    throw new NoSuchElementException("No more tuples");
                }

                // 获取下一个聚合结果
                Map.Entry<Field, Integer> entry = it.next();
                Tuple tuple = new Tuple(finalTd); // 假设getTupleDesc()方法返回正确的TupleDesc

                // 根据是否有分组，创建并返回相应的tuple
                if (isGrouping()) {
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                } else {
                    tuple.setField(0, new IntField(entry.getValue()));
                }

                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // 重置迭代器
                it = aggregates.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return finalTd;
            }

            @Override
            public void close() {
                // 关闭迭代器
                it = null;
            }

            private boolean isGrouping() {
                return !(gbfield == NO_GROUPING && type == null);
            }
        };
    }

}
