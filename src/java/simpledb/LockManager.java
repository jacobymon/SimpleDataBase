package simpledb;


//import I added
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit; 
import java.util.Collections; // Added for thread safety in getTransactionsHoldingLock()

/**
 * Manages locks on PageIds held by TransactionIds.
 * S-locks and X-locks are represented as Permissions.READ_ONLY and Permisions.READ_WRITE, respectively
 *
 * All the field read/write operations are protected by this
 */
public class LockManager {
    
    private final Map<PageId, Map<TransactionId, Permissions>> pageLocks;
    private final Map<TransactionId, Set<PageId>> transactionLocks;
    
    // EXERCISE 5: WAITS-FOR GRAPH DATA STRUCTURE
    // Key: The waiting transaction (tid)
    // Value: The set of transactions (otherTid) that tid is waiting for.
    private final Map<TransactionId, Set<TransactionId>> waitsForGraph;

    // Increased LOCK_WAIT time is no longer strictly necessary with cycle detection, 
    // but keep it for wait() timeout.
    final int LOCK_WAIT = 50;       // milliseconds 
    
    /**
     * Sets up the lock manager to keep track of page-level locks for transactions
     */
    public LockManager() {
        this.pageLocks = new HashMap<>(); 
        this.transactionLocks = new HashMap<>();
        this.waitsForGraph = new HashMap<>(); // Initialize the graph
	    
    }
    
    /**
     * Tries to acquire a lock on page pid for transaction tid, with permissions perm. 
     * If cannot acquire the lock, blocks and waits.
     */
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
	throws DeadlockException {
	    
	synchronized(this) { 
        
        while(!lock(tid, pid, perm)) { 
            
            // --- EXERCISE 5: DEADLOCK CYCLE DETECTION ---

            // 1. Identify all transactions currently holding conflicting locks
            // NOTE: The logic here needs to accurately capture the holders, including other readers 
            // if we are attempting an S->X upgrade.
            Set<TransactionId> holders = getTransactionsHoldingLock(tid, pid, perm); // Pass tid as well

            // 2. Add waiting edges to the Waits-For Graph
            waitsForGraph.putIfAbsent(tid, new HashSet<>());
            waitsForGraph.get(tid).addAll(holders);

            // 3. Check for a cycle
            if (hasCycle(tid)) {
                // Remove the waiting edges before throwing the exception
                waitsForGraph.remove(tid);
                throw new DeadlockException();
            }

            try {
                // Wait until notified or until timeout
                this.wait(LOCK_WAIT); 
                
                // If we wake up, remove the waiting edges (as tid is no longer actively waiting)
                waitsForGraph.remove(tid);
                
            } catch (InterruptedException e) { 
                waitsForGraph.remove(tid);
                throw new DeadlockException(); 
            }
        }
        
        // After successfully acquiring the lock, ensure tid is not marked as waiting
        waitsForGraph.remove(tid);
        return true;
    }
    }
    
    /**
     * Helper method to find all transactions that currently block the requested lock.
     * Only called when a conflict is found in lock() -> locked().
     * This method must be robust enough to handle the lock upgrade case.
     */
    private Set<TransactionId> getTransactionsHoldingLock(TransactionId tid, PageId pid, Permissions perm) {
        Set<TransactionId> holders = new HashSet<>();
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        
        if (locksOnPage == null) return Collections.emptySet();

        for (Map.Entry<TransactionId, Permissions> entry : locksOnPage.entrySet()) {
            TransactionId otherTid = entry.getKey();
            Permissions existingPerm = entry.getValue();

            // Case 1: Standard Conflict - Lock requested is X, or existing is X
            // This covers all X-lock conflicts.
            if (perm == Permissions.READ_WRITE || existingPerm == Permissions.READ_WRITE) {
                if (!otherTid.equals(tid)) {
                    holders.add(otherTid);
                }
            } 
            
            // Case 2: Lock Upgrade Conflict - T1 is upgrading (S->X) and blocked by T2 (S)
            // If the requesting transaction (tid) has an S-lock, and wants an X-lock, 
            // and another transaction (otherTid) also holds an S-lock, tid must wait for otherTid.
            else if (perm == Permissions.READ_WRITE && !otherTid.equals(tid)) {
                 // Check if the current tid holds an S-lock on this page
                 // Since we are iterating over locksOnPage, we need to check if tid has S-lock
                 // This requires a separate check because otherTid is T2, not T1.

                 // The simplified check: if otherTid holds ANY lock, and T1 is requesting X, T1 might be waiting.
                 // We rely on the 'locked' method's return of TRUE to indicate a valid blocking state.
                 // In the upgrade scenario (T1 has S, T2 has S, T1 requests X), T1 is blocked by T2's S-lock.
                 
                 // Since we know we are blocked (because lock() returned false), and we are iterating 
                 // over holders, we check if otherTid is a non-compatible holder.

                 // If T1 wants X-lock AND T1 has S-lock AND T2 has S-lock, T1 waits for T2.
                 // The 'existingPerm' for otherTid is S, so the main conflict 'if' above is false.
                 
                 // CRITICAL FIX: Explicitly include the upgrade block rule in the holder check.
                 // T1 requests X, T2 holds S: T1 must wait for T2.
                 if (existingPerm == Permissions.READ_ONLY && perm == Permissions.READ_WRITE) {
                      holders.add(otherTid);
                 }
            }
        }
        return holders;
    }

    /**
     * Helper method to perform Depth First Search (DFS) for cycles in the Waits-For Graph.
     */
    private boolean hasCycle(TransactionId tid) {
        // Recursive DFS call, tracking visited nodes in current path and overall visited nodes.
        // We initialize 'visited' inside the helper, but the base call needs a clean start.
        return hasCycleHelper(tid, new HashSet<>(), new HashSet<>());
    }

    private boolean hasCycleHelper(TransactionId currentTid, Set<TransactionId> path, Set<TransactionId> visited) {
        if (path.contains(currentTid)) {
            // Cycle detected: currentTid is already in the current recursion path.
            return true;
        }

        if (visited.contains(currentTid)) {
            // Already checked this subtree, no cycle here.
            return false;
        }

        path.add(currentTid);
        visited.add(currentTid);

        Set<TransactionId> dependencies = waitsForGraph.get(currentTid);
        if (dependencies != null) {
            for (TransactionId dependencyTid : dependencies) {
                if (hasCycleHelper(dependencyTid, path, visited)) {
                    return true;
                }
            }
        }

        path.remove(currentTid); // Backtrack
        return false;
    }

    // --- Original methods below ---
    
    /**
     * Release all locks corresponding to TransactionId tid (Strict 2PL).
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = transactionLocks.get(tid);
        
        if (pages != null) {
            // CRITICAL FIX: Iterate over a copy to avoid ConcurrentModificationException
            Set<PageId> pagesToRelease = new HashSet<>(pages); 

            for (PageId pid : pagesToRelease) {
                releaseLock(tid, pid);
            }
        }
        // Ensure that transaction is not waiting if it was aborted/completed
        waitsForGraph.remove(tid);
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        return locksOnPage != null && locksOnPage.containsKey(tid);
    }
    
    
    /**
     * Answers the question: is this transaction "locked out" of acquiring lock on this page with this perm?
     */
    private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        
        // Rule 1: If no one has a lock, there's no conflict.
        if (locksOnPage == null || locksOnPage.isEmpty()) {
            return false;
        }

        // Iterate through all existing lock entries
        for (Map.Entry<TransactionId, Permissions> entry : locksOnPage.entrySet()) {
            TransactionId otherTid = entry.getKey();
            Permissions existingPerm = entry.getValue();

            // Case 1: Lock held by the requesting transaction (tid)
            if (otherTid.equals(tid)) {
                
                // Trivial: tid already holds X-lock, or is re-requesting S-lock. NO conflict.
                if (existingPerm == Permissions.READ_WRITE || perm == Permissions.READ_ONLY) {
                    return false;
                }
                
                // C. Lock Upgrade Check (S -> X): Must be the SOLE holder
                if (perm == Permissions.READ_WRITE && existingPerm == Permissions.READ_ONLY) {
                    // Conflict if there is MORE than just this transaction's lock (size > 1).
                    return locksOnPage.size() > 1; 
                }
            } 
            
            // Case 2: Lock held by a different transaction (otherTid != tid)
            else {
                // CONFLICT RULE: Block if either the request is X, or the existing lock is X.
                // S vs S is the only compatible non-self interaction.
                if (perm == Permissions.READ_WRITE || existingPerm == Permissions.READ_WRITE) {
                    return true; // Conflict found, block immediately.
                }
            }
        }
    
    // If the loop completes without finding any reason to block, the lock can be acquired.
    return false;
    }

    
    /**
     * Releases whatever lock this transaction has on this page.
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
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
        
        // WAKE UP THREADS: Notify any waiting threads that a lock has been released
        this.notifyAll(); 
       
    }
    
    
    /**
     * Attempt to lock the given PageId with the given Permissions for this TransactionId
     */
    private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
        if(locked(tid, pid, perm)) 
            return false; // Conflict found (locked out)

        // Lock is granted: update pageLocks and transactionLocks
        pageLocks.putIfAbsent(pid, new HashMap<>());
        pageLocks.get(pid).put(tid, perm); 

        transactionLocks.putIfAbsent(tid, new HashSet<>());
        transactionLocks.get(tid).add(pid); 

        return true;
    }
        
}
