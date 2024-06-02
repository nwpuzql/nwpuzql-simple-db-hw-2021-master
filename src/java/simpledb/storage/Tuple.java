package simpledb.storage;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc tupleDesc;
    private List<Field> fields;
    private RecordId recordId;
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.fields = new ArrayList<Field>();
        this.tupleDesc = td;
//        this.recordId = new RecordId(new HeapPageId(Integer.MIN_VALUE, Integer.MIN_VALUE), Integer.MIN_VALUE);
        // some code goes here
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0) {
            throw new IndexOutOfBoundsException("Index: " + i + " is out of bounds.");
        }

        // 确保fields列表大小至少能包含索引i的位置
        // 如果fields的大小小于或等于i，扩展fields列表以包含足够的元素
        while (this.fields.size() <= i) {
            this.fields.add(null); // 使用null作为占位符
        }

        // 设置或替换索引i的位置的Field元素
        this.fields.set(i, f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        return this.fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder buf = new StringBuilder();
        for (Field f : this.fields) {
            buf.append(f.toString());
        }
        return buf.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
    }

    public static Tuple merge(Tuple t1, Tuple t2) {
        List<Field> f = new ArrayList<>(t1.fields);
        f.addAll(t2.fields);
        Tuple t = new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
        t.fields = f;
        return t;
    }
}
