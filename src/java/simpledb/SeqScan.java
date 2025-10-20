package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid; // Transaction this scan is part of
    private int tableid;       // Table ID to scan
    private String tableAlias; // Alias for the table
    private DbFileIterator dbFileIterator; // Iterator over the HeapFile
    private TupleDesc tupleDesc; // TupleDesc with table alias

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table.
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias == null ? "null" : tableAlias;

        // Initialize the iterator
        dbFileIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);

        // Set the TupleDesc with table alias
        tupleDesc = createAliasTupleDesc(Database.getCatalog().getDatabaseFile(tableid).getTupleDesc(), this.tableAlias);
    }

    /**
     * Alternative constructor with default table alias.
     */
    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * @return the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Resets the tableid, and tableAlias of this operator.
     * 
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table.
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias == null ? "null" : tableAlias;
        dbFileIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        tupleDesc = createAliasTupleDesc(Database.getCatalog().getDatabaseFile(tableid).getTupleDesc(), this.tableAlias);
    }

    /**
     * Open the iterator.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        dbFileIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * Check if there are more tuples to read.
     */
    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (dbFileIterator == null) {
            throw new IllegalStateException("Iterator not open");
        }
        return dbFileIterator.hasNext();
    }

    /**
     * Return the next tuple.
     */
    @Override
    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        if (dbFileIterator == null) {
            throw new IllegalStateException("Iterator not open");
        }
        return dbFileIterator.next();
    }

    /**
     * Rewind the iterator.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if (dbFileIterator == null) {
            throw new IllegalStateException("Iterator not open");
        }
        dbFileIterator.rewind();
    }

    /**
     * Close the iterator.
     */
    @Override
    public void close() {
        // dbFileIterator.close();
        // dbFileIterator = null;
        if (dbFileIterator != null) {
            dbFileIterator.close();
            dbFileIterator = null;  // Reset the iterator
        }
        // super.close();
    }

    /**
     * Helper method to create a TupleDesc with table alias prefixed to field names.
     */
    private TupleDesc createAliasTupleDesc(TupleDesc originalTd, String alias) {
        int numFields = originalTd.numFields();
        Type[] types = new Type[numFields];
        String[] fieldNames = new String[numFields];

        for (int i = 0; i < numFields; i++) {
            types[i] = originalTd.getFieldType(i);
            String originalName = originalTd.getFieldName(i);
            fieldNames[i] = alias + "." + originalName;
        }

        return new TupleDesc(types, fieldNames);
    }
}
