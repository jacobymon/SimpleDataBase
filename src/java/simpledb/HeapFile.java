package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;

    private TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;

        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
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
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNumber = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        byte[] pageData = new byte[pageSize];
        
        try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
            // Seek to the page offset in the file
            file.seek(pageNumber * pageSize);
            
            // Read the page data
            file.readFully(pageData);
            
            // Create and return a HeapPage
            return new HeapPage((HeapPageId) pid, pageData);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File not found: " + f.getName(), e);
        } catch (IOException e) {
            throw new RuntimeException("IOException while reading page", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        int pageNumber = pid.getPageNumber();
        
        // turn page into bytes
        byte[] pageData = page.getPageData();
        
        // Open in a file to put the data into
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        
        try {
            // Move the file pointer to the correct page offset
            file.seek(pageNumber * BufferPool.getPageSize());
            
            // put page data into file
            file.write(pageData);
        } finally {
            file.close();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        //to keep track of the pages we modify        
        ArrayList<Page> affectedPages = new ArrayList<>(); 

        // find a valid page
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            // Check if page has room for the tuple
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                affectedPages.add(page); // Add affected page
                return affectedPages; // Return the affected pages immediately
            }
        }

        // No pages with space, create a new one
        HeapPageId newPid = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        
        // Write page to disk
        writePage(newPage);
        
        // Note that we dont have explicitly add to the buffer pool as `getPage` already handles that
        affectedPages.add(newPage); 
        return affectedPages; 
        }

        // see DbFile.java for javadocs
        public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
                TransactionAbortedException {
            ArrayList<Page> dirtiedPages = new ArrayList<>();
            
            // Find the page w/ the tuple we are deleting
            PageId pid = t.getRecordId().getPageId();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            
            // delete tuple
            page.deleteTuple(t);
            page.markDirty(true, tid); 
            dirtiedPages.add(page);
            
            return dirtiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private int currentPageIndex = 0;
            private Iterator<Tuple> tupleIterator = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                currentPageIndex = 0;
                tupleIterator = getPageTuples(currentPageIndex);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // Throw IllegalStateException if the iterator hasn't been opened
                if (tupleIterator == null) {
                    throw new IllegalStateException("Iterator not opened");
                }

                // Check if there are more tuples in the current page
                while (!tupleIterator.hasNext()) {
                    currentPageIndex++;
                    if (currentPageIndex >= numPages()) return false;
                    tupleIterator = getPageTuples(currentPageIndex);
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                // Throw IllegalStateException if the iterator hasn't been opened
                if (tupleIterator == null) {
                    throw new IllegalStateException("Iterator not opened");
                }
                if (!hasNext()) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                tupleIterator = null;
            }

            private Iterator<Tuple> getPageTuples(int pageIndex) throws TransactionAbortedException, DbException {
                PageId pageId = new HeapPageId(getId(), pageIndex);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                return page.iterator();
            }
        };
    }
}

