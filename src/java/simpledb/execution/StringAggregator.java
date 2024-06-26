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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type type;
    private int afield;
    private Op op;
    private Map<Field, Integer> aggregates;
    private TupleDesc td = null;


    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.type = gbfieldtype;
        this.afield = afield;
        this.op = what;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator OP != COUNT");
        }
        this.aggregates = new HashMap<Field, Integer>();
        if (gbfield == NO_GROUPING && type == null) {  // 没有group by子句，tuple和tupleDesc都只有一个字段
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{type, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
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
        if (!aggregates.containsKey(gbf)) {
            aggregates.put(gbf, 1);
        } else {
            int aRes = aggregates.get(gbf);
            switch (this.op) {
                case COUNT:
                    aRes++;
                    aggregates.put(gbf, aRes);
                    break;
                default:
                    throw new IllegalStateException("unknown op: " + this.op);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
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
