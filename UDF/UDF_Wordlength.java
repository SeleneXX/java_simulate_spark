package UDF;
import java.util.ArrayList;
import java.util.HashMap;
import code.MapReduceUDf;


public class UDF_Wordlength implements MapReduceUDf {

    @Override
    public ArrayList<HashMap<String, Integer>> Map(String Key, String Value) {
        ArrayList<HashMap<String, Integer>> list = new ArrayList<HashMap<String, Integer>>();
        for (String key:Value.split(" ")){
            HashMap<String, Integer> pair = new HashMap<String, Integer>();
            int len = key.length();
            pair.put(String.valueOf(len), 1);
            list.add(pair);
        }
        return list;
    }

    @Override
    public HashMap<String, Integer> Reduce(ArrayList<HashMap<String, Integer>> list){
        HashMap<String,Integer> reduce_list = new HashMap<String,Integer>();
        for(HashMap<String,Integer> word:list){
            ArrayList<String> keys =  new ArrayList<String>(word.keySet());
            for (String key : keys) {
                Integer value = word.get(key);
                Integer reduce_value = reduce_list.putIfAbsent(key, value);
                if(reduce_value != null){
                    reduce_list.put(key, reduce_value+value);
                }
            }
        }
        return reduce_list;  
    }
    
}
