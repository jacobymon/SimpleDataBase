package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;

    private boolean fetched;
    private TupleDesc td;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        this.fetched = false;


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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method. You can pass along the TransactionId from the constructor.
     * This operator should keep track of whether its fetchNext() method has been called already. 
     * 
     * @return A 1-field tuple containing the number of deleted records (even if there are 0)
     *          or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetched) {
            return null;
        
        }

        fetched = true;
        int deleteCount = 0;

        while (child.hasNext()) {
            Tuple tupleToDelete = child.next();
            try{
                Database.getBufferPool().deleteTuple(tid, tupleToDelete);
                deleteCount++;
            }
            catch (IOException e) {
                throw new DbException("deletion failed");
            }
        }

        Tuple result = new Tuple(td);
        result.setField(0, new IntField(deleteCount));

        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("can only be one child operator");
        }
        child = children[0];
    }
    
}
