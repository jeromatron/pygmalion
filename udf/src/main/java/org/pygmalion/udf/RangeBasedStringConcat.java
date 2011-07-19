package org.pygmalion.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * This class is capable of concatenating specific items in a tuple together, as well as
 * the whole tuple.  It will also recurse on DataBags and Tuples and concat those together
 *
 * Usage: RangeBasedStringConcat("1,2,3", " "), RangeBasedStringConcat("ALL", " ");
 */
//TODO: switch to byte based approach
public class RangeBasedStringConcat extends EvalFunc<String> {
    public static final String ALL = "all";
    public static final String DEFAULT_SEPARATOR = " ";
    private String range;
    private int[] ranges;
    private String separator = DEFAULT_SEPARATOR;

    public RangeBasedStringConcat() {
        this(ALL, DEFAULT_SEPARATOR);
    }

    /**
     * If the range is empty or "ALL", then concat all values.  Else, a comma separated list
     * of the fields to concat.
     * @param range comma separated list of field numbers for the tuple, else ALL
     */
    public RangeBasedStringConcat(String range, String separator) {
        this.range = range;
        this.separator = separator;
        initRange();

    }

    private void initRange() {
        //TODO: add support for ranges like 1-10
        if (range != null && range.equalsIgnoreCase(ALL) == false){
            String [] splits = range.split(",");
            ranges = new int[splits.length];
            for (int i = 0; i < splits.length; i++) {
                ranges[i] = Integer.parseInt(splits[i]);
            }
        }
    }

    @Override
    public String exec(Tuple input) throws IOException {
        int tupleSize = input.size();
        if (input == null || tupleSize == 0) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        if (range != null && range.equalsIgnoreCase(ALL)){
            processTuple(input, builder);
        } else {
            for (int theRange : ranges) {
                if (theRange < tupleSize) {
                    appendObject(input.get(theRange), builder);
                }
            }
        }
        //remove the trailing separate
        return builder.length() > 0 ? builder.substring(0, builder.length() -1) : "";
    }

    private void processTuple(Tuple input, Appendable builder) throws IOException {
        for (Object o : input.getAll()) {
            appendObject(o, builder);
        }
    }

    private void appendObject(Object o, Appendable builder) throws IOException {
        if (o instanceof Tuple){
            Tuple tmp = (Tuple) o;
            if (tmp.size() > 0){
                processTuple(tmp, builder);
            }
        } else if (o instanceof DataBag){
            DataBag db = (DataBag) o;
            for (Tuple tuple : db) {
                processTuple(tuple, builder);
            }
        } else {
            builder.append(o.toString()).append(separator);
        }
    }
}
