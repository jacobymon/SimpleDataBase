package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // TODO: some code goes here
    }

    public Predicate getPredicate() {
        // TODO: some code goes here
        return null;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return null;
    }

    public void open() throws DbException, NoSuchElementException,
    TransactionAbortedException {
        // TODO: some code goes here
    }

    public void close() {
        // TODO: some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
    }

    /**
     * The Filter operator iterates through the tuples from its child, 
     * applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * This method returns the next tuple.
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
    TransactionAbortedException, DbException {
        // TODO: some code goes here
        return null;
    }

    /**
     * See Operator.java for additional notes 
     */
    @Override
    public DbIterator[] getChildren() {
        // TODO: some code goes here
        return null;
    }

    /**
     * See Operator.java for additional notes 
     */
    @Override
    public void setChildren(DbIterator[] children) {
        // TODO: some code goes here
    }

}
