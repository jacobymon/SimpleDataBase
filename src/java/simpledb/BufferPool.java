package simpledb;

import java.io.*;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import java.util.List;
import java.util.Iterator;
import java.util.LinkedList; // Added for LRU tracking
import java.util.HashSet;
import java.util.Set;
import java.util.Collection;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private int numPages;

    private final Map<PageId, Page> map;
    
    // Added LRU list to track page order for eviction
    private final LinkedList<PageId> lruList; 

    private final LockManager lockmgr;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.map = new ConcurrentHashMap<>();
        this.lruList = new LinkedList<>();
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
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // 1. ACQUIRE LOCK: Must happen first. LockManager handles blocking/waiting.
        try {
            lockmgr.acquireLock(tid, pid, perm);
        } catch (DeadlockException e) { 
            throw new TransactionAbortedException(); 
        }

        // 2. LOOK UP PAGE
        if (this.map.containsKey(pid)) {
            Page page = this.map.get(pid);
            // Update LRU access
            lruList.remove(pid);
            lruList.addFirst(pid);
            return page;
        }

        // 3. PAGE MISS (Fetch from disk, evict if necessary, and add)
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);

        // 4. EVICTION CHECK (Must happen before putting the page in)
        if (this.map.size() >= this.numPages) {
            evictPage(); 
        }

        // 5. CACHE INSERT
        this.map.put(pid, page);
        this.lruList.addFirst(pid);
        return page;
    }


    /**
     * Releases the lock on a page.
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockmgr.releaseLock(tid,pid); 
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // FORCE policy helper is called here
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
        
        // 1. DEAL WITH DIRTY PAGES (FORCE / NO UNDO)
        if (commit) {
            // FORCE policy: Write all dirty pages for this transaction to disk on commit
            flushPages(tid); 
        } else {
            // NO UNDO / ABORT: Throw away all dirty pages associated with this transaction
            
            // Get all PageIds dirty by this transaction
            Set<PageId> pagesToDiscard = new HashSet<>();
            for (Map.Entry<PageId, Page> entry : map.entrySet()) {
                Page page = entry.getValue();
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    pagesToDiscard.add(entry.getKey());
                }
            }
            
            // Discard the pages from the buffer pool
            for (PageId pid : pagesToDiscard) {
                // Since this page was dirty, we remove it to revert changes.
                // The next access to this page will read the old, clean version from disk.
                discardPage(pid); 
            }
        } 
        
        // 2. STRICT 2PL: Release all locks after commit/abort
        lockmgr.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> affectedPages = file.insertTuple(tid, t);

        for (Page page : affectedPages) {
            page.markDirty(true, tid);
            
            if (!map.containsKey(page.getId()) && map.size() >= numPages) {
                evictPage();  
            }
            map.put(page.getId(), page);
            lruList.remove(page.getId());
            lruList.addFirst(page.getId());
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        
        List<Page> affectedPages = file.deleteTuple(tid, t);

        for (Page page : affectedPages) {
            page.markDirty(true, tid);
            
            if (!map.containsKey(page.getId()) && map.size() >= numPages) {
                evictPage();  
            }
            map.put(page.getId(), page);
            lruList.remove(page.getId());
            lruList.addFirst(page.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : map.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
    */
    public synchronized void discardPage(PageId pid) {
        map.remove(pid);
        lruList.remove(pid);
    }

    /**
     * Flushes (i.e., writes) a certain page to disk by asking 
     * the correct HeapFile to write the page
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = map.get(pid);
        if (page == null) {
            return; 
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

    /** Write all pages of the specified transaction to disk (FORCE POLICY).
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // Implementation for the FORCE policy: write all pages dirtied by tid to disk.
        // Iterate over a copy of keys to avoid ConcurrentModificationException during flushPage
        Set<PageId> pageIds = new HashSet<>(map.keySet());
        for (PageId pid : pageIds) {
            Page page = map.get(pid);
            if (page != null && page.isDirty() != null && page.isDirty().equals(tid)) {
                // flushPage will write the page and mark it clean
                flushPage(pid); 
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Implements the NO STEAL policy: must never evict a dirty page.
     */
    private synchronized void evictPage() throws DbException {
        PageId pageToEvict = null;
        
        // Iterate through pages based on the LRU list (from least recently used)
        for (PageId pid : lruList) { 
            Page page = map.get(pid);

            // NO STEAL: Only evict if the page is NOT dirty (isDirty() returns null)
            if (page.isDirty() == null) {
                pageToEvict = pid;
                break;
            }
        }

        if (pageToEvict == null) {
            // Buffer pool is full of dirty pages that cannot be evicted (NO STEAL)
            throw new DbException("BufferPool is full: Cannot evict any dirty page (NO STEAL policy).");
        }

        try {
            // Flush the clean page (safe since it's not dirty, but call flushPage just in case)
            flushPage(pageToEvict); 
        } catch (IOException e) {
            throw new DbException("Error flushing page during eviction: " + e.getMessage());
        }

        // Remove the page from the buffer pool and LRU list
        map.remove(pageToEvict);
        lruList.remove(pageToEvict);
    }
}