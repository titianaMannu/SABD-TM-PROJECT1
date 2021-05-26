package utils.comparators;


import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class Tuple2Comparator<K,V> implements Comparator<Tuple2<K, Double>>, Serializable {
    @Override
    public int compare(Tuple2<K, Double> tupleA, Tuple2<K, Double> tupleB) {
        return tupleA._2().compareTo(tupleB._2());
    }

}