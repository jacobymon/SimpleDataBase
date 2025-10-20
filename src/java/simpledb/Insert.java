package simpledb;

import java.io.IOException; //necessary import for doing try catch with IOExceptions

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private boolean fetched;
    private TupleDesc td;


    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        this.fetched = false;

        TupleDesc tableTd = Database.getCatalog().getTupleDesc(tableid);
        if (!child.getTupleDesc().equals(tableTd)) {
            throw new DbException("TupleDesc of child is different from the table's TupleDesc");
        }

        this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        fetched = false;

    }

    public void close() {
        super.close();
        child.close();

    }

    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        fetched = false;
    }

    /**
     * Inserts tuples read from child into the relation with the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records (even if there are 0!). 
     * Insertions should be passed through BufferPool.insertTuple() with the 
     * TransactionId from the constructor. An instance of BufferPool is available via 
     * Database.getBufferPool(). Note that insert DOES NOT need to check to see if 
     * a particular tuple is a duplicate before inserting it.
     *
     * This operator should keep track if its fetchNext() has already been called, 
     * returning null if called multiple times.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetched) {
            return null;
        }

        fetched = true;
        int insertCount = 0;

        while (child.hasNext()) {
            Tuple tupleToInsert =child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableid, tupleToInsert);
                insertCount++;

            } catch(IOException e) {
                throw new DbException("insertion failed");
            }
        }
        Tuple result = new Tuple(td);
        result.setField(0, new IntField(insertCount));

        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("expected one child operator");
        }
        child = children[0];
    }
}
