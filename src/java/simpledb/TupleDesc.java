package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    // A list to store TDItem instances
    private final List<TDItem> tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;

    // Default constructor that initializes an empty list for the TDItems
    public TupleDesc() {
        this.tdItems = new ArrayList<>();
    }

    // Constructor that creates a TupleDesc based on the provided types and field names
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must contain at least one entry.");
        }
        this.tdItems = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            String fieldName = (fieldAr != null && i < fieldAr.length) ? fieldAr[i] : null;
            tdItems.add(new TDItem(typeAr[i], fieldName));
        }
    }

    // Constructor with only typeAr array
    public TupleDesc(Type[] typeAr) {
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must contain at least one entry.");
        }
        this.tdItems = new ArrayList<>();
        for (Type type : typeAr) {
            tdItems.add(new TDItem(type, null));
        }
    }

    // Constructor with a list of TDItems
    public TupleDesc(List<TDItem> tdItems) {
        this.tdItems = new ArrayList<>(tdItems);
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
    }

    // Other methods...

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= tdItems.size()) {
            throw new NoSuchElementException("Index " + i + " is not a valid field reference.");
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= tdItems.size()) {
            throw new NoSuchElementException("Index " + i + " is not a valid field reference.");
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * No match if name is null.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tdItems.size(); i++) {
            if (name != null && name.equals(tdItems.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException("No field with the name '" + name + "' found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     * @see Type#getSizeInBytes
     */
    public int getSizeInBytes() {
        int bytes = 0;
        for (TDItem item : tdItems) {
            bytes += item.fieldType.getSizeInBytes();
        }
        return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TDItem> mergedItems = new ArrayList<>(td1.tdItems);
        mergedItems.addAll(td2.tdItems);
        return new TupleDesc(mergedItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i. It does not matter if the field names are equal.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if (this.numFields() != other.numFields()) {
            return false;
        }
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < this.numFields(); i++) {
            result.append(this.getFieldName(i)).append(" (").append(this.getFieldType(i)).append(")");
            if (i < this.numFields() - 1) {
                result.append(", ");
            }
        }
        return result.toString();
    }

}

