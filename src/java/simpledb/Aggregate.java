package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min).
 * Note that we only support aggregates over a single column, grouped by a single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private DbIterator aggIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of fetchNext().
     * 
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping.
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        // Determine if it's an integer or string field
        TupleDesc childTD = child.getTupleDesc();
        Type afieldType = childTD.getFieldType(afield);

        // Create the appropriate aggregator (Integer or String)
        if (afieldType == Type.INT_TYPE) {
            Type gbfieldType = (gfield == Aggregator.NO_GROUPING) ? null : childTD.getFieldType(gfield);
            this.aggregator = new IntegerAggregator(gfield, gbfieldType, afield, aop);
        } else if (afieldType == Type.STRING_TYPE) {
            Type gbfieldType = (gfield == Aggregator.NO_GROUPING) ? null : childTD.getFieldType(gfield);
            this.aggregator = new StringAggregator(gfield, gbfieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return null;
     */
    public String groupFieldName() {
        return (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        super.open();
        child.open();

        // Merge all tuples from the child into the aggregator
        while (child.hasNext()) {
            Tuple tuple = child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }

        // Get the iterator from the aggregator and open it
        aggIterator = aggregator.iterator();
        aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggIterator != null && aggIterator.hasNext()) {
            return aggIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     */
    public TupleDesc getTupleDesc() {
        TupleDesc childTD = child.getTupleDesc();
        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{nameOfAggregatorOp(aop) + " (" + aggregateFieldName() + ")"});
        } else {
            return new TupleDesc(new Type[]{childTD.getFieldType(gfield), Type.INT_TYPE},
                    new String[]{groupFieldName(), nameOfAggregatorOp(aop) + " (" + aggregateFieldName() + ")"});
        }
    }

    public void close() {
        super.close();
        child.close();
        aggIterator.close();
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length > 0) {
            child = children[0];
        }
    }
}
