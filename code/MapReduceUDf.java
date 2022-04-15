package code;

import java.util.ArrayList;
import java.util.HashMap;

public interface MapReduceUDf{
    public ArrayList<HashMap<String,Integer>> Map (String Key , String Value);
    public HashMap<String,Integer> Reduce(ArrayList<HashMap<String,Integer>> list);
}