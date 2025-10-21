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
    
    // private final Map<PageId, Set<TransactionId>> readLocks = new HashMap<>();
    // private final Map<PageId, TransactionId> writeLocks = new HashMap<>();

    private final Map<PageId, Map<TransactionId, Permissions>> pageLocks;
    private final Map<TransactionId, Set<PageId>> transactionLocks;

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
	    
	// while(!lock(tid, pid, perm)) { // keep trying to get the lock
	    
	//     synchronized(this) {
	// 	// you don't have the lock yet
	// 	// possibly some code here for Exercise 5, deadlock detection
	//     }
	    
	//     try {
	// 	// couldn't get lock, wait for some time, then try again
	// 	Thread.sleep(LOCK_WAIT); 
	//     } catch (InterruptedException e) { // do nothing
	//     }
	    
	// }
	    
	// synchronized(this) {
	//     // for Exercise 5, might need some cleanup on deadlock detection data structure
	// }
	    
	// return true;

    synchronized(this) { // Synchronize on the LockManager instance
        while(!lock(tid, pid, perm)) { 
            // Lock is NOT acquired. Must wait.
            
            // NOTE: Insert Exercise 5 deadlock check logic here
            
            try {
                // Wait until another thread calls notifyAll() (in releaseLock)
                this.wait(); 
                
                // If we wake up, the loop checks the lock condition again.
                
            } catch (InterruptedException e) { 
                // Do nothing or re-throw
            }
        }
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
            // !!! CRITICAL FIX: CREATE A COPY TO ITERATE OVER !!!
            // This copy ensures the iterator doesn't crash when releaseLock modifies the original set.
            Set<PageId> pagesToRelease = new HashSet<>(pages); 

            for (PageId pid : pagesToRelease) {
                releaseLock(tid, pid);
            }
        }
	    
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        return locksOnPage != null && locksOnPage.containsKey(tid);
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
        
        // Rule: If no one has a lock on this page, there's no conflict.
        if (locksOnPage == null || locksOnPage.isEmpty()) {
            return false;
        }

        // Iterate through all transactions (otherTid) that currently hold a lock on pid.
        for (Map.Entry<TransactionId, Permissions> entry : locksOnPage.entrySet()) {
            TransactionId otherTid = entry.getKey();
            Permissions existingPerm = entry.getValue();

            // Case 1: Checking if the lock is held by the requesting transaction (tid).
            if (otherTid.equals(tid)) {
                // A. If tid holds an X-lock (READ_WRITE), it can get any lock (including another X-lock).
                // B. If tid holds an S-lock (READ_ONLY), it can get another S-lock.
                if (existingPerm == Permissions.READ_WRITE || perm == Permissions.READ_ONLY) {
                    return false;
                }
                
                // C. Lock Upgrade Check: If tid holds an S-lock and wants an X-lock,
                //    it's allowed ONLY IF it's the sole holder of a lock on the page.
                if (perm == Permissions.READ_WRITE && existingPerm == Permissions.READ_ONLY) {
                    // If the requesting transaction is the ONLY one on the page, allow the upgrade.
                    if (locksOnPage.size() == 1){
                        return false; //not locked
                    } else {
                        return true; // not the only lock, so theres a conflict and it's locked
                    }
                }
                // If any other compatible case is covered, it falls through to the end (return false).
            } 
            
            // Case 2: Checking for conflicts with another transaction (otherTid != tid).
            else {
                // Fundamental Conflict Rule: Any lock request conflicts if either the existing
                // lock or the requested lock is an X-lock (READ_WRITE).
                // A Read lock (S) conflicts with a Write lock (X), and a Write lock (X) conflicts with everything.
                if (perm == Permissions.READ_WRITE || existingPerm == Permissions.READ_WRITE) {
                    return true; // Conflict found with a different transaction.
                }
            }
        }
    
    // If the loop completes without finding any conflicts, the lock can be acquired.
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
        //release the lock held by tid on pid
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        if (locksOnPage != null) {
            locksOnPage.remove(tid);
            if (locksOnPage.isEmpty()) {
                pageLocks.remove(pid);
            }
        }

        // Remove the page from the transaction's lock set
        Set<PageId> pages = transactionLocks.get(tid);
        if (pages != null) {
            pages.remove(pid);
            if (pages.isEmpty()) {
                transactionLocks.remove(tid);
            }
        }
        notifyAll(); // Notify any waiting threads that a lock has been released
       
    }
    
        
    
    
    /**
     * Attempt to lock the given PageId with the given Permissions for this TransactionId
     * Should update the lock table data structure(s) if successful
     *
     * Returns true if the lock attempt was successful, false otherwise
     */
    private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
        if(locked(tid, pid, perm)) // Path 1: Conflict Found
        return false; // <-- RETURNS FALSE HERE

        // Path 2: No Conflict (Lock can be acquired or upgraded)
        // Acquire lock: update pageLocks and transactionLocks
        pageLocks.putIfAbsent(pid, new HashMap<>());
        pageLocks.get(pid).put(tid, perm); // Lock is acquired/upgraded

        transactionLocks.putIfAbsent(tid, new HashSet<>());
        transactionLocks.get(tid).add(pid); // Lock is tracked

        return true;
    }
        
}