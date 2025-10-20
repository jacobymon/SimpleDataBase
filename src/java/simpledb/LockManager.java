package simpledb;


//import I added
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Manages locks on PageIds held by TransactionIds.
 * S-locks and X-locks are represented as Permissions.READ_ONLY and Permisions.READ_WRITE, respectively
 *
 * All the field read/write operations are protected by this
 */
public class LockManager {
    
    private final Map<PageId, Set<TransactionId>> readLocks = new HashMap<>();
    private final Map<PageId, TransactionId> writeLocks = new HashMap<>();

    final int LOCK_WAIT = 10;       // milliseconds
    
    /**
     * Sets up the lock manager to keep track of page-level locks for transactions
     * Should initialize state required for the lock table data structure(s)
     */
    public LockManager() {
        this.pageLocks = new HashMap<>(); //keep track of which transactions hold which type of locks on each page.
        this.transactionLocks = new HashMap<>();
	    
    }
    
    /**
     * Tries to acquire a lock on page pid for transaction tid, with permissions perm. 
     * If cannot acquire the lock, waits for a timeout period, then tries again. 
     * This method does not return until the lock is granted, or an exception is thrown
     *
     * In Exercise 5, checking for deadlock will be added in this method
     * Note that a transaction should throw a DeadlockException in this method to 
     * signal that it should be aborted.
     *
     * @throws DeadlockException after on cycle-based deadlock
     */
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
	throws DeadlockException {
	    
	while(!lock(tid, pid, perm)) { // keep trying to get the lock
	    
	    synchronized(this) {
		// you don't have the lock yet
		// possibly some code here for Exercise 5, deadlock detection
	    }
	    
	    try {
		// couldn't get lock, wait for some time, then try again
		Thread.sleep(LOCK_WAIT); 
	    } catch (InterruptedException e) { // do nothing
	    }
	    
	}
	    
	synchronized(this) {
	    // for Exercise 5, might need some cleanup on deadlock detection data structure
	}
	    
	return true;
    }
    
    /**
     * Release all locks corresponding to TransactionId tid.
     * This method is used by BufferPool.transactionComplete()
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
	Set<PageId> pages = transactionLocks.get(tid);
    if (pages != null) {
        for (PageId pid : pages) {
            releaseLock(tid, pid);
        }
    }
	    
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        // Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        // return locksOnPage != null && locksOnPage.containsKey(tid);
        return (writeLocks.get(pid) != null && writeLocks.get(pid).equals(tid)) ||
               (readLocks.get(pid) != null && readLocks.get(pid).contains(tid));
    }
    }
    
    /**
     * Answers the question: is this transaction "locked out" of acquiring lock on this page with this perm?
     * Returns false if this tid/pid/perm lock combo can be achieved (i.e., not locked out), true otherwise.
     * 
     * Logic:
     *
     * if perm == READ_ONLY
     *  if tid is holding any sort of lock on pid, then the tid can acquire the lock (return false).
     *
     *  if another tid is holding a READ lock on pid, then the tid can acquire the lock (return false).
     *  if another tid is holding a WRITE lock on pid, then tid can not currently 
     *  acquire the lock (return true).
     *
     * else
     *   if tid is THE ONLY ONE holding a READ lock on pid, then tid can acquire the lock (return false).
     *   if tid is holding a WRITE lock on pid, then the tid already has the lock (return false).
     *
     *   if another tid is holding any sort of lock on pid, then the tid cannot currenty acquire the lock (return true).
     */
    private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {
    Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
    
    // No locks on this page, so no conflict
    if (locksOnPage == null) {
        return false;
    }

    for (Map.Entry<TransactionId, Permissions> entry : locksOnPage.entrySet()) {
        TransactionId otherTid = entry.getKey();
        Permissions existingPerm = entry.getValue();

        if (otherTid.equals(tid)) {
            // If this transaction already holds a compatible lock, allow it
            if (perm == Permissions.READ_ONLY || existingPerm == Permissions.READ_WRITE) {
                return false;
            } else if (perm == Permissions.READ_WRITE && existingPerm == Permissions.READ_ONLY && locksOnPage.size() == 1) {
                // Transaction can upgrade if it's the only one with a READ lock
                return false;
            }
        } else {
            // Check conflicts with other transactions
            if (perm == Permissions.READ_WRITE || existingPerm == Permissions.READ_WRITE) {
                return true;
            }
        }
    }
    
    return false;
}
    
    /**
     * Releases whatever lock this transaction has on this page
     * Should update lock table data structure(s)
     *
     * Note that you do not need to "wake up" another transaction that is waiting for a lock on this page,
     * since that transaction will be "sleeping" and will wake up and check if the page is available on its own
     * However, if you decide to change the fact that a thread is sleeping in acquireLock(), you would have to wake it up here
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
	// Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
    // if (locksOnPage != null) {
    //     locksOnPage.remove(tid);
    //     if (locksOnPage.isEmpty()) {
    //         pageLocks.remove(pid);
    //     }
    // }

    // Set<PageId> pages = transactionLocks.get(tid);
    // if (pages != null) {
    //     pages.remove(pid);
    //     if (pages.isEmpty()) {
    //         transactionLocks.remove(tid);
    //     }
    // }

    // Remove the write lock if held by this transaction
        if (writeLocks.get(pid) != null && writeLocks.get(pid).equals(tid)) {
            writeLocks.remove(pid);
        }
        // Remove the read lock if held by this transaction
        if (readLocks.get(pid) != null) {
            readLocks.get(pid).remove(tid);
            // Clean up the set if empty
            if (readLocks.get(pid).isEmpty()) {
                readLocks.remove(pid);
            }
        }
	    
    }
    
    /**
     * Attempt to lock the given PageId with the given Permissions for this TransactionId
     * Should update the lock table data structure(s) if successful
     *
     * Returns true if the lock attempt was successful, false otherwise
     */
    private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
	    
	// if(locked(tid, pid, perm)) 
	//     return false; // this transaction cannot get the lock on this page; it is "locked out"

	// // Else, this transaction is able to get the lock, update lock table
	// // Acquire lock: update pageLocks and transactionLocks
    // pageLocks.putIfAbsent(pid, new HashMap<>());
    // pageLocks.get(pid).put(tid, perm);

    // transactionLocks.putIfAbsent(tid, new HashSet<>());
    // transactionLocks.get(tid).add(pid);

    // return true;

    if (perm == Permissions.READ_ONLY) {
            // Check if there's an existing write lock by another transaction
            if (writeLocks.containsKey(pid) && !writeLocks.get(pid).equals(tid)) {
                return false; // Another transaction has the write lock, deny access
            }
            // Grant read lock by adding this transaction to the read lock set
            readLocks.computeIfAbsent(pid, k -> new HashSet<>()).add(tid);
            return true; // Lock acquired
        } else { // For READ_WRITE permission
            // Check for conflicting locks (any read locks or a write lock by another transaction)
            if ((readLocks.containsKey(pid) && !readLocks.get(pid).contains(tid)) ||
                (writeLocks.containsKey(pid) && !writeLocks.get(pid).equals(tid))) {
                return false; // Conflict exists, deny access
            }
            // Grant write lock, clearing any read locks held by this transaction
            readLocks.remove(pid); // Remove all read locks for this page
            writeLocks.put(pid, tid); // Grant write lock to this transaction
            return true; // Lock acquired
        }
    }
}