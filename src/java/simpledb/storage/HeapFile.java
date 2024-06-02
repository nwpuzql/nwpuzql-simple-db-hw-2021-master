package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
//    public int firstFreePageNo;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
//        this.firstFreePageNo = -1;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile file = null;
        try {
            // 创建一个RandomAccessFile对象用于读取
            file = new RandomAccessFile(this.file, "r");

            // 计算要读取的页面的起始位置
            int position = pid.getPageNumber() * BufferPool.getPageSize();

            // 将文件指针移动到起始位置
            file.seek(position);

            // 创建一个用于存放页面数据的字节数组
            byte[] data = new byte[BufferPool.getPageSize()];

            // 读取页面数据
            file.readFully(data);

            // 根据读取到的数据创建一个Page对象并返回
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 确保文件被正确关闭
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // 如果页面不存在或读取过程中发生错误，抛出IllegalArgumentException
        throw new IllegalArgumentException("Unable to read the page from disk");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        RandomAccessFile file = null;
        try {
            // 创建一个RandomAccessFile对象用于写入
            file = new RandomAccessFile(this.file, "rw");

            // 计算要写入的页面的起始位置
            int position = pid.getPageNumber() * BufferPool.getPageSize();

            // 将文件指针移动到起始位置
            file.seek(position);

            // 创建一个用于存放页面数据的字节数组
            byte[] data = page.getPageData();

            file.write(data);
        } catch (IOException e) {
            throw e;
        } finally {
            // 确保文件被正确关闭
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 获取文件的总大小（字节为单位）
        long fileSize = this.file.length();

        // 获取每个页面的大小
        int pageSize = BufferPool.getPageSize(); // 获取页面大小

        // 计算页面数量，注意使用长整型来避免潜在的溢出
        return (int) (fileSize + pageSize - 1) / pageSize;
    }

    /*
     * 只有在无可用页面的情况下，调用此函数进行新增页面
     */
    private HeapPageId addPage() throws IOException {
        HeapPageId newPid = new HeapPageId(this.getId(), this.numPages());
        Page newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        this.writePage(newPage);
        return newPid;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        HeapPageId newPid = null;
        HeapPage page = null;
        for (int i = 0; i < numPages(); i++) {  // 由于头歌不允许更改DbFile类，只能从头遍历每一页是否有空槽位
            newPid = new HeapPageId(this.getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
            if (page.getFirstEmptySlotNo() != page.numSlots) {  // 当前遍历的页面还有空闲
                t.setRecordId(new RecordId(newPid, Integer.MIN_VALUE));
                page.insertTuple(t);
                return Arrays.asList(page);
            }
        }
        newPid = this.addPage();
        page = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_ONLY);
        t.setRecordId(new RecordId(newPid, rid.getTupleNumber()));
        page.insertTuple(t);
        return Arrays.asList(page);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    public Iterator<Tuple> getEachPageIt(TransactionId tid, PageId pid) throws TransactionAbortedException, DbException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);  //find the page via pid
        return page.iterator();       //return the tuples in the page with id pid
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new AbstractDbFileIterator() {
            private final int numPage = numPages();
            private int pageNo = 0;
            private PageId pid = null;
            private Iterator<Tuple> it = null;   // 页面的iterator

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (it == null) return null;
                while (!it.hasNext()) {  //
                    if (pageNo < numPage - 1) {
                        pageNo++;
                        open();
                    } else {
                        return null;
                    }
                }
                return it.next();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                // 获取当前page的it
                try {
                    pid = new HeapPageId(getId(), pageNo);
                    it = getEachPageIt(tid, pid);
                } catch (Exception e) {
                    throw new DbException("there are problems opening/accessing the database: " + e.getMessage());
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageNo = 0;
                open();
            }

            @Override
            public void close() {
                super.close();
                it = null;
            }
        };
    }

}

