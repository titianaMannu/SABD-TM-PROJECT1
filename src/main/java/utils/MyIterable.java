package utils;
import java.io.Serializable;
import java.util.*;

public class MyIterable implements Serializable {

    private final List<Object> list;

    public MyIterable(Object tuple2) {
        list = new ArrayList<>();
        list.add(tuple2);
    }

    public void add(Object t){
        list.add(t);
    }

    public MyIterable addAll(MyIterable I){
        list.addAll(I.getList());
        return this;
    }

    public List getList(){
        return list;
    }

    public void descendingSort(Comparator c){
        Collections.sort(this.list, c);
        Collections.reverse(this.list);
    }

    public void sublist(int start, int end){
        ArrayList<Object> temp = new ArrayList<>();
        for (int i = start; i < end; i++){
            temp.add(list.get(i));
        }
        this.list.clear();
        this.list.addAll(temp);
    }

    @Override
    public String toString() {
        return "MyIterable{" +
                "list=" + list +
                '}';
    }
}
