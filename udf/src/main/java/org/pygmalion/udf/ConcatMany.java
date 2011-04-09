package org.pygmalion.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class ConcatMany extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (Object o : input.getAll()) {
            builder.append((String) o);
        }

        return builder.toString();
    }
}
