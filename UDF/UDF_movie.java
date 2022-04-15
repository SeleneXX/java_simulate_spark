package UDF;
import java.util.ArrayList;
import java.util.HashMap;
import code.MapReduceUDf;

public class UDF_movie implements MapReduceUDf {

    @Override
    public ArrayList<HashMap<String, Integer>> Map(String Key, String Value){
        ArrayList<HashMap<String, Integer>> list = new ArrayList<HashMap<String, Integer>>();
        for (String row:Value.split(" ")){
            if(row.equals("")){break;}
            String[] content = row.split(",");
            boolean is =content[0].equals("4439");
            if(is){
                HashMap<String, Integer> pair = new HashMap<String, Integer>();
                pair.put(content[1],1);
                list.add(pair);
            }
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
