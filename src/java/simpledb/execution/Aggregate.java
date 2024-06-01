package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield;
    private int gbfield;
    private Aggregator.Op op;
    private Aggregator aggregator;
    OpIterator it;  // 这个算子全局的迭代器，来自于聚合器

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gbfield = gfield;
        this.op = aop;
        TupleDesc td = child.getTupleDesc();
        Type aType = td.getFieldType(afield);
        Type gbType = null;
        if (gbfield != Aggregator.NO_GROUPING) {
            gbType = td.getFieldType(gbfield);
        }
        // 根据聚合的列的类型（int/string）来选择构造函数
        if (aType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gbType, afield, aop);
        } else if (aType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gfield, gbType, afield, aop);
        }

        try {
            child.open();
            while (child.hasNext()) {
                aggregator.mergeTupleIntoGroup(child.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean hasGroupBy() {
        return !(gbfield == -1);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gbfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (!hasGroupBy()) {
            return null;
        } else {
            return child.getTupleDesc().getFieldName(gbfield);
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        it = aggregator.iterator();  // 实例化迭代器
        it.open();  // 打开迭代器
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it.hasNext()) {
            Tuple t = it.next();
            t.resetTupleDesc(getTupleDesc());  // 重新设置td
            return t;
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] typeArr = null;
        String[] fieldNameArr = null;
        String aFieldName = String.format("aggName(%s) (%s)", op.toString(), aggregateFieldName());
        TupleDesc td = child.getTupleDesc();
        if (!hasGroupBy()) {
            typeArr = new Type[]{Type.INT_TYPE};
            fieldNameArr = new String[]{aFieldName};
        } else {
            Type gbType = td.getFieldType(gbfield);
            typeArr = new Type[]{gbType, Type.INT_TYPE};
            fieldNameArr = new String[]{groupFieldName(), aFieldName};
        }
        return new TupleDesc(typeArr, fieldNameArr);
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
