package simpledb;

import java.io.*;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import java.util.List;



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
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private int numPages;

    private final Map<PageId, Page> map;

    private final LockManager lockmgr;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /** TODO for Lab 4: create instance of Lock Manager class. 
	Be sure to instantiate it in the constructor. */

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        // maybe make it a dictionary key is Pid and value is page where we can map page IDs to pages
        this.map = new ConcurrentHashMap<>();
        
        this.lockmgr = new LockManager();



    }
    
    public static int getPageSize() {
      return pageSize;
    }

    /**
     * Helper: this should be used for testing only!!!
     */
    public static void setPageSize(int pageSize) {
	BufferPool.pageSize = pageSize;
    }
    /**
     * Helper: this should be used for testing only!!!
     */
    public static void resetPageSize(int pageSize) {
	BufferPool.pageSize = PAGE_SIZE;
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {


        try {
        lockmgr.acquireLock(tid, pid, perm);
        } catch (DeadlockException e) { 
            throw new TransactionAbortedException(); 
        }

        if (this.map.containsKey(pid)) {
            return this.map.get(pid);  
        }

        //get the page
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);

        //check if there's room in the buffer, if not evict a page
        if (this.map.size() >= this.numPages) {
            evictPage();
        }

        //put the page in if there's room
        this.map.put(pid, page);
        return page;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        

	lockmgr.releaseLock(tid,pid); // Uncomment for Lab 4
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {


	return lockmgr.holdsLock(tid, p); 
        
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        
        if (commit) {
        flushPages(tid);
        }
        lockmgr.releaseAllLocks(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        // Get the file for the table we are inserting the tuple into
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        List<Page> affectedPages = file.insertTuple(tid, t);

        // For each affected page, mark it as dirty and add it to the buffer pool
        for (Page page : affectedPages) {
            page.markDirty(true, tid);
            if (map.size() >= numPages) {
                evictPage();  
            }
            map.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        
        List<Page> affectedPages = file.deleteTuple(tid, t);

        for (Page page : affectedPages) {
            page.markDirty(true, tid);
            if (map.size() >= numPages) {
                evictPage();  
            }
            map.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : map.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for labs 1--4
    }

    /**
     * Flushes (i.e., writes) a certain page to disk by asking 
     * the correct HeapFile to write the page
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = map.get(pid);
        if (page == null) {
            throw new IOException("Page not found in the buffer pool");
        }

        // write dirty page to disk
        TransactionId dirtyTid = page.isDirty();
        if (dirtyTid != null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbFile.writePage(page);
            //mark the page clean since we wrote it to disk
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for labs 1--4
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId pageToEvict = null;

        // find a clean page to evict
        for (PageId pid : map.keySet()) {
            Page page = map.get(pid);
            if (page.isDirty() == null) {
                pageToEvict = pid;
                break;
            }

        }
    

    if (pageToEvict == null) {
        for (PageId pid : map.keySet()) {
            pageToEvict = pid;
            break;
        }
    }

    try {
        flushPage(pageToEvict);
    } catch (IOException e) {
        throw new DbException("Error flushing page during eviction: " + e.getMessage());
    }

    map.remove(pageToEvict);
    }
}
