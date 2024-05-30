package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableID;
    private String tableAlias;
    private DbFileIterator tupleIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableID = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableID);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableID = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableID);
            tupleIterator = dbFile.iterator(tid);
            tupleIterator.open();
        } catch (DbException e) {
            throw new DbException("there are problems opening/accessing the database: " + e.getMessage());
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // 获取原始的TupleDesc
        TupleDesc td = Database.getCatalog().getTupleDesc(tableID);
        int numFields = td.numFields();

        // 创建新的类型数组和字段名数组
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];

        // 对每个字段，复制类型，并添加表别名前缀到字段名
        for (int i = 0; i < numFields; i++) {
            typeAr[i] = td.getFieldType(i);
            String name = td.getFieldName(i);
            fieldAr[i] = tableAlias + "." + name;
        }

        // 返回新的TupleDesc
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (tupleIterator == null) {
            throw new IllegalStateException("Iterator not open");
        }
        return tupleIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (tupleIterator == null) {
            throw new IllegalStateException("Iterator not open");
        }
        return tupleIterator.next();
    }

    public void close() {
        // some code goes here
        if (tupleIterator != null) {
            tupleIterator.close();
            tupleIterator = null;
        }
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (tupleIterator == null) {
            throw new IllegalStateException("Iterator not open");
        }
        tupleIterator.rewind();
    }
}
