package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private static int numPages = DEFAULT_PAGES;

    private Map<PageId, Page> pages;
    private Map<PageId, TransactionId> transactions;
    private Map<PageId, Permissions> permissions;

    // LRU算法淘汰页的数据结构
    private List<PageId> LRUQueue;     // 按使用时间远近排列
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        BufferPool.numPages = numPages;
        this.pages = new HashMap<PageId, Page>();
        this.LRUQueue = new ArrayList<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        Page page = pages.get(pid);
        // 页面在缓冲池中
        if (page != null) {
            LRUQueue.remove(pid);                         // 删除pid
            LRUQueue.add(pid);                            // 从队尾重新插入页面号
            return page;
        }
        // 页面不在缓冲池中,从HeapFile读取page
        page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        if (pages.size() < pageSize) {
            LRUQueue.add(pid);                       // 从队尾插入
            pages.put(pid, page);
            return page;
        } else {
            throw new DbException("the page is full and there is no implemented evict method!");
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)        // 上层的接口，统一通过调用缓冲池这个方法来插入tuple
            throws DbException, IOException, TransactionAbortedException {  // 内部获取DBFile，通过其insertTuple方法插入
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        for (Page page : dirtyPages) {            // 对于插入操作影响的每一页
            page.markDirty(true, tid);            // 更新dirty位
            if (this.pages.size() == numPages) {  // 检查当前缓冲池是否已满
                this.evictPage();                 // 如果已满，则写回一页
            }
            PageId pid = page.getId();
            LRUQueue.add(pid);                       // 从队尾插入
            this.pages.put(page.getId(), page);   // 将新页加入缓冲池
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        int tableId = pid.getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.deleteTuple(tid, t);  // 将tuple从表文件中删除
        for (Page page : dirtyPages) {                     // 对于删除操作影响的每一页
            page.markDirty(true, tid);               // 更新dirty位
            if (this.pages.size() == numPages) {           // 检查当前缓冲池是否已满
                this.evictPage();                          // 如果已满，则写回一页
            }
            LRUQueue.add(pid);                       // 从队尾插入
            this.pages.put(page.getId(), page);             // 将新页加入缓冲池
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // 创建pages的keySet的副本
        Set<PageId> pageIds = new HashSet<>(pages.keySet());
        for (PageId pid : pageIds) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        // 删除LRU队列，page列表
        LRUQueue.remove(pid);
        this.pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        // 将页面写回disk，同时标记重置dirty bit
        Page page = pages.get(pid);
        page.markDirty(false, new TransactionId());
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(pages.get(pid));

        // 删除LRU，hash，page队列
        LRUQueue.remove(pid);
        this.pages.remove(pid);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId pid = LRUQueue.get(0);
        try {
            flushPage(pid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
