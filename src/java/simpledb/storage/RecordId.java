package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private final PageId pageId;
    private final int tupleNo;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid the pageid of the page on which the tuple resides
     * @param tno the tuple number within the page.
     */
    public RecordId(PageId pid, int tno) {
        // some code goes here
        this.pageId = pid;
        this.tupleNo = tno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return this.tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof RecordId)) {
            return false;
        }
        RecordId other = (RecordId) o;
        return this.pageId.equals(other.pageId) && this.tupleNo == other.tupleNo;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        final int prime = 31; // 选择一个质数作为基数
        int result = 1;

        // 假设PageId类也正确地实现了hashCode()
        result = prime * result + ((pageId == null) ? 0 : pageId.hashCode());
        result = prime * result + tupleNo;

        return result;

    }

}
