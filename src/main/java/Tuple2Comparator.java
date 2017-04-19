import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by antonkozmirchuk on 19/04/17.
 */
public class Tuple2Comparator implements Comparator<Tuple2<Integer, String>>, Serializable, scala.Serializable {

    @Override
    public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
        int r = o1._1.compareTo(o2._1);

        if (r == 0)
            r = o1._2.compareTo(o2._2);

        return r;
    }
}
