package org.pygmalion.udf.uuid;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.UUID;

/**
 * Generates a time UUID. Either give it a time to use or
 * don't specify one and get a time UUID based on the current
 * time.
 */
public class GenerateTimeUUID extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        String rval = null;

        if (input != null || input.size() == 0) {
            rval = TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString();
        } else if (input.size() == 1) {
            Long time = (Long) input.get(0);
            rval = TimeUUIDUtils.getTimeUUID(time).toString();
        } else
            throw new IOException("You must either have no argument or one argument (time:long) to use GenerateTimeUUID");

        return rval;
    }
}
