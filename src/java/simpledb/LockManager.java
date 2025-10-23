package simpledb;


//import I added
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit; 
import java.util.Collections; 

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
    private final Map<TransactionId, Set<TransactionId>> waitsForGraph;

    final int LOCK_WAIT = 50;       // milliseconds 
    
    /**
     * Sets up the lock manager to keep track of page-level locks for transactions
     */
    public LockManager() {
        this.pageLocks = new HashMap<>(); 
        this.transactionLocks = new HashMap<>();
        this.waitsForGraph = new HashMap<>(); 
    }
    
    /**
     * Tries to acquire a lock on page pid for transaction tid, with permissions perm. 
     * If cannot acquire the lock, blocks and waits.
     */
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
    throws DeadlockException {
        
        synchronized(this) { 
            long startTime = System.currentTimeMillis();
            
            while(!lock(tid, pid, perm)) { 
                // Add waiting edges to graph
                Set<TransactionId> holders = getTransactionsHoldingLock(tid, pid, perm); 
                waitsForGraph.putIfAbsent(tid, new HashSet<>());
                waitsForGraph.get(tid).addAll(holders);

                // Check for deadlock
                if (hasCycle(tid)) {
                    waitsForGraph.remove(tid);
                    throw new DeadlockException();
                }

                try {
                    wait(LOCK_WAIT);
                } catch (InterruptedException e) {
                    waitsForGraph.remove(tid);
                    throw new DeadlockException();
                }

                // Check for timeout
                if (System.currentTimeMillis() - startTime > 1000) { // 1 second timeout
                    waitsForGraph.remove(tid);
                    throw new DeadlockException();
                }
            }
            
            waitsForGraph.remove(tid);
            return true;
        }
    }
    
    /**
     * Helper method to find all transactions that currently block the requested lock.
     */
    private Set<TransactionId> getTransactionsHoldingLock(TransactionId tid, PageId pid, Permissions perm) {
        Set<TransactionId> holders = new HashSet<>();
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        
        if (locksOnPage == null) return Collections.emptySet();

        for (Map.Entry<TransactionId, Permissions> entry : locksOnPage.entrySet()) {
            TransactionId otherTid = entry.getKey();
            Permissions existingPerm = entry.getValue();

            // Ignore the transaction that is requesting the lock (self-check)
            if (otherTid.equals(tid)) {
                continue;
            }

            // CONFLICT RULE: Block if requested is X OR existing is X. 
            if (perm == Permissions.READ_WRITE || existingPerm == Permissions.READ_WRITE) {
                 holders.add(otherTid);
            } 
        }
        return holders;
    }

    /**
     * Helper method to perform Depth First Search (DFS) for cycles in the Waits-For Graph.
     */
    private boolean hasCycle(TransactionId tid) {
        return hasCycleHelper(tid, new HashSet<>(), new HashSet<>());
    }

    private boolean hasCycleHelper(TransactionId currentTid, Set<TransactionId> path, Set<TransactionId> visited) {
        if (path.contains(currentTid)) {
            return true;
        }

        if (visited.contains(currentTid)) {
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

        path.remove(currentTid);
        return false;
    }

    // --- Original methods below ---
    
    /**
     * Release all locks corresponding to TransactionId tid (Strict 2PL).
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = transactionLocks.get(tid);
        
        if (pages != null) {
            Set<PageId> pagesToRelease = new HashSet<>(pages); 

            for (PageId pid : pagesToRelease) {
                releaseLock(tid, pid);
            }
        }
        waitsForGraph.remove(tid);
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        return locksOnPage != null && locksOnPage.containsKey(tid);
    }
    
    
    /**
     * Answers the question: is this transaction "locked out" of acquiring lock on this page with this perm?
     *
     * IMPORTANT: This method determines blocking behavior.
     */
    private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {
        Map<TransactionId, Permissions> locksOnPage = pageLocks.get(pid);
        
        if (locksOnPage == null || locksOnPage.isEmpty()) {
            return false;
        }

        // Check existing lock by this transaction
        Permissions existingPerm = locksOnPage.get(tid);
        if (existingPerm != null) {
            // Already has the exact lock needed
            if (existingPerm.equals(perm)) {
                return false;
            }
            // Upgrade case (S->X)
            if (perm == Permissions.READ_WRITE && existingPerm == Permissions.READ_ONLY) {
                return locksOnPage.size() > 1;
            }
            // Downgrade case (X->S) or same permission
            return false;
        }

        // Check conflicts with other transactions
        for (Map.Entry<TransactionId, Permissions> entry : locksOnPage.entrySet()) {
            if (entry.getValue() == Permissions.READ_WRITE || 
                perm == Permissions.READ_WRITE) {
                return true;
            }
        }
        
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
