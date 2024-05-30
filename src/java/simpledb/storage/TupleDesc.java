package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        /**
         * Returns the size (in bytes) of the field type.
         */
        public int getSize() {
            return fieldType.getLen();
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // 检查tdItems是否已初始化，如果没有，则进行初始化
        this.tdItems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            this.tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // 进行初始化
        this.tdItems = new ArrayList<TDItem>();
        for (Type type : typeAr) {
            this.tdItems.add(new TDItem(type, "")); // fieldName为空
        }
        // some code goes here
    }

    public TupleDesc(List<TDItem> tdItems) {
        this.tdItems = tdItems;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.tdItems.size()) {
            throw new NoSuchElementException("Index " + i + " is out of bounds.");
        } else {
            return this.tdItems.get(i).fieldName;
        }
        // some code goes here
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.tdItems.size()) {
            throw new NoSuchElementException("Index " + i + " is out of bounds.");
        } else {
            return this.tdItems.get(i).fieldType;
        }
        // some code goes here
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null || name.isEmpty()) {
            throw new NoSuchElementException("Name is null or empty.");
        }
        for (int i = 0; i < this.tdItems.size(); i++) {
            if (this.tdItems.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("Field name " + name + " not found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for (TDItem item : this.tdItems) {
            totalSize += item.getSize();
        }
        // some code goes here
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if (td1 == null || td2 == null) {
            throw new IllegalArgumentException("TupleDesc arguments cannot be null");
        }

        List<TDItem> mergedList = new ArrayList<>(td1.tdItems);
        mergedList.addAll(td2.tdItems);

        // some code goes here
        return new TupleDesc(mergedList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // Step 1: Check for null
        if (o == null) {
            return false;
        }

        // Step 2: Check for the correct type
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        // Step 3: Typecast to TupleDesc
        TupleDesc other = (TupleDesc) o;

        // Step 4: Check if the number of fields are the same
        if (this.numFields() != other.numFields()) {
            return false;
        }

        // Step 5: Check if all types are the same
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.tdItems.get(i).fieldType.equals(other.tdItems.get(i).fieldType)) {
                return false;
            }
        }

        // Step 6: All checks have passed
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int result = 1;
        for (int i = 0; i < this.numFields(); i++) {
            result = 31 * result + (this.tdItems.get(i).fieldType.hashCode());
        }
        return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (TDItem tdItem : this.tdItems) {
            String fieldString = tdItem.toString();
            s.append(fieldString + ",");
        }
        if (s.length() > 0) { // 删除最后一个','
            s.deleteCharAt(s.length() - 1);
        }
        return s.toString();
        // some code goes here
    }
}
