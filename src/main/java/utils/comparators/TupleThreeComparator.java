package utils.comparators;


import javaslang.Tuple3;


import java.io.Serializable;
import java.util.Comparator;

public class TupleThreeComparator<K,V> implements Serializable, Comparator<Tuple3<Object, Object, Object>> {

    @Override
    public int compare(Tuple3 ta, Tuple3 tb) {
        return ta.compareTo(tb);
    }
}
