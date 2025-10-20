package simpledb;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

/**
 * Computes some aggregate over a set of StringFields.
 * The only operator supported for StringFields is COUNT.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // HashMap for storing the count of grouped tuples
    //Note that only count can be supported for strings
    private HashMap<Field, Integer> aggregateValues;

    /**
     * Aggregate constructor
     * 
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // Only COUNT is allowed for StringFields, raise exception if other ops are used
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT is supported for StringAggregator.");
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregateValues = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor.
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField;

        //no grouping
        if (this.gbfield == NO_GROUPING) {
            groupField = null;
        } else {
            groupField = tup.getField(this.gbfield); 
        }

        // in case group DNE
        if (!aggregateValues.containsKey(groupField)) {
            aggregateValues.put(groupField, 0); 
        }

        aggregateValues.put(groupField, aggregateValues.get(groupField) + 1);
    }

    /**
     * Returns a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if using group,
     *         or a single (aggregateVal) if no grouping.
     */
    public DbIterator iterator() {
        List<Tuple> resultTuples = new ArrayList<>();

        TupleDesc resultTD;

        if (this.gbfield == NO_GROUPING) {
            resultTD = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            resultTD = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }


        // Create the result tuples based on the aggregated counts
        for (Map.Entry<Field, Integer> entry : aggregateValues.entrySet()) {
            Tuple resultTuple = new Tuple(resultTD);

            if (this.gbfield == NO_GROUPING) {
                resultTuple.setField(0, new IntField(entry.getValue()));
            } else {
                // group-by field
                resultTuple.setField(0, entry.getKey());
                // count 
                resultTuple.setField(1, new IntField(entry.getValue())); 
            }

            resultTuples.add(resultTuple);
        }
        return new TupleIterator(resultTD, resultTuples);
    }

}
