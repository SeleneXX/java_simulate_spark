package code;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;


public class Worker extends Thread {

    private String task_name;
    private MapReduceUDf UDF;
    private static final String Output_File_floder = "output/";
    private String output_file;
    private static final String Intermediate_File_floder = "code/temp/";
    private boolean ismapper;
    private boolean sorted;
    private String input_fileSplit;
    private int num_partition;
    private Pipe pipe;
    // used for fault tolerance test
    private static final boolean fault = true; // on or of fault tolerance testing
    private static final boolean error = false; // if this worker occurs an error 
    private static final double chance_stop = 15;  // [0,100] of chance or rate  that this works stop working for no reason
    // there still be chances that worker do not stop unless chance_stop = 100
    // run more times to if you dont see !!!message!!!



    // mapping task woker
    Worker(MapReduceUDf UDF, String input_fileSplit, Pipe pipe, String task_name, int num_partition, boolean ismapper) {
        //mapper
        this.task_name = task_name;
        this.ismapper = ismapper;
        this.UDF = UDF;
        this.input_fileSplit = input_fileSplit;
        this.pipe = pipe;
        this.num_partition = num_partition;
        this.sorted=false;
    }

    // reducing task woker
    Worker(MapReduceUDf UDF, Pipe pipe, String task_name,int num_partition ,boolean ismapper) {
        //reducer
        if(ismapper) {
            System.out.println("Get reducer info but used as mapper");
        }
        this.task_name = task_name;
        this.ismapper = ismapper;
        this.sorted = false;
        this.UDF = UDF;
        this.pipe = pipe;
        this.num_partition = num_partition;
    }

    // sorting task worker
    Worker(Pipe pipe,String task_name, String output_file,boolean sorted){
        this.pipe = pipe;
        this.ismapper =false;
        this.task_name = task_name;
        this.sorted = sorted;
        this.output_file = output_file;
    }

    // send message to master
    private void sendMessage(String message) {
        try {
            //System.out.println("** Message from "+this.getName()+" send:" + message+"**");
            Pipe.SinkChannel sinkChannel = pipe.sink();
            ByteBuffer buf = ByteBuffer.allocate(128);
            buf.clear();
            buf.put(message.getBytes());
            buf.flip();
            while (buf.hasRemaining()) {
                sinkChannel.write(buf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // pause This Thread for a while, InterruptedException fixed
    private void pauseThisThread(int m) {
        try {
            this.sleep(m);
        } catch (InterruptedException e) {
            e.printStackTrace();
            sendMessage(this.getName()+e.getMessage());
        }
    }

    // read a txt file and return a String
    private String read_txt(String InputFile) {
        String txt = "";
        try {
            String encoding = "UTF-8";
            File file = new File(InputFile);
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    txt = txt + lineTxt + " ";
                }
                read.close();
            } else {
                System.out.println("File not found.");
                sendMessage(this.getName()+"File not found.");
                return null;
            }
        } catch (Exception e) {
            System.out.println("Read in fail");
            e.printStackTrace();
            sendMessage(this.getName()+e.getMessage());
            return null;
        }
        return txt;
    }

    // write an object to .dat file. use as midle file
    private boolean writeObjectToFile(Object obj, String floder_adress, String file_name) {
        File floder = new File(floder_adress);
        if (!floder.exists()) {
            floder.mkdirs();// creat file
        }
        file_name = floder_adress + "/" + file_name + ".dat";
        File file = new File(file_name);
        try {
            FileOutputStream out = new FileOutputStream(file);
            ObjectOutputStream objOut = new ObjectOutputStream(out);
            objOut.writeObject(obj);
            objOut.flush();
            objOut.close();
        } catch (Exception e) {
            sendMessage("erro in "+this.getName() + " write object failed"+e.getMessage());
            return false;
        }
        return true;
    }

    // write output to a txt file
    private boolean toTxtFile(HashMap<String, Integer> pairs,ArrayList<String> order, String output_file) {
        String filename = Output_File_floder + this.task_name + "out.txt";;
        String floder_address = Output_File_floder;
        String encoding = "UTF-8";
        // creat father floder
        if(output_file!=null&&!output_file.equals("")){
            filename = output_file;
            String[] p = output_file.split("/");
            String end = p[p.length-1];
            floder_address = output_file.replaceAll(end, "");
        }
        try {
            File floder = new File(floder_address);
            if (!floder.exists()) {
                floder.mkdirs();// creat flie
            }
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename),encoding));
            for(String word_key: order ){
                Integer value = pairs.get(word_key);
                out.write(word_key + " " + value.toString() + "\r\n");
            }
            out.close();
        } catch (Exception e) {
        }
        return true;
    }

    // hash_algorithm for dividing String key to R partition
    private int hash_algorithm(String key) {
        if(key.equals("")||key.equals(" ")){return 0;}
        char first_char = Character.toLowerCase(key.charAt(0));
        int value = Integer.valueOf(first_char);
        return value % this.num_partition;
    }

    // seperate k-v pairs to each partition in the end of mapping
    private ArrayList<HashMap<String, Integer>>[] partition_split(ArrayList<HashMap<String, Integer>> list) {
        // ArrayList[] to store k-v pairs for each partition
        ArrayList<HashMap<String, Integer>>[] partition_lists = new ArrayList[this.num_partition];
        // Explicitly declare the variables of each array to prevent null pointer errors
        for (int i = 0; i < this.num_partition; i++){          
            partition_lists[i] = new ArrayList<HashMap<String, Integer>>();
        }
        // Assign each K-V pair to each patition
        for (HashMap<String, Integer> pair : list) {
            ArrayList<String> keys = new ArrayList<String>(pair.keySet());
            String key = keys.get(0);
            // use this to not count "" and " "
            if(key.equals("")||key.equals(" ")){continue;}
            int partition = hash_algorithm(key);
            partition_lists[partition].add(pair);
        }
        return partition_lists;
    }

    // read all intermediate file in one partition (only read .dat file)
    private ArrayList<HashMap<String, Integer>> readPartition() throws Exception{
        String file_floder =Intermediate_File_floder + task_name + "/partition-" + this.num_partition;
        File file =new File(file_floder);
        String[] datas = file.list();
        ArrayList<HashMap<String, Integer>> list = new ArrayList<HashMap<String, Integer>>();
        for(String data: datas){
            Object obj =readdat(file_floder+"/"+data);
            if(obj == null){return null;}
            list.addAll((ArrayList<HashMap<String,Integer>>) obj);
        }
        return list;
    }
    
    // read one intermediate file(.dat) to Object
    private Object readdat(String file_name){
        Object temp=null;
        File file =new File(file_name);
        FileInputStream in;
        try {
            in = new FileInputStream(file);
            ObjectInputStream objIn=new ObjectInputStream(in);
            temp=objIn.readObject();
            objIn.close();
        } catch (Exception e) {
            sendMessage("error in "+this.getName()+e.getMessage());
            System.out.println("read object fail in "+this.getName());
            return null;
        }
        return temp;
    }

    // sort the output of reduce
    private boolean sort_output(){
        try{
            String file_floder =Intermediate_File_floder+this.task_name+"/reduce_output";
            File file =new File(file_floder);
            String[] datas = file.list();
            HashMap<String, Integer> pairs = new HashMap<String, Integer>();
            for(String data: datas){
                pairs.putAll((HashMap<String, Integer>)readdat(file_floder+"/"+data));
            }
            ArrayList<String> keys = new ArrayList<String>(pairs.keySet());
            keys.sort(Comparator.naturalOrder());
            if(toTxtFile(pairs,keys,this.output_file)){
                return true;
            }
            return false;
        }catch(Exception e){
            sendMessage("error in"+this.getName()+" Sorting: "+e.getMessage());
            return false;
        }      
    }
    
    // invoke map UDF and write pairs to each partition
    private boolean map(String value) {
        // Map by UDF
        ArrayList<HashMap<String, Integer>> list =new ArrayList<HashMap<String,Integer>>();
        try {
            if(error){
                throw new Exception("Fault tolerance test");
            }
            list = UDF.Map("", value);
            if(list==null){
                sendMessage("error in"+this.getName()+"UDF has erro, return a null list");
                return false;
            } 
            ArrayList<HashMap<String, Integer>>[] partition_list = partition_split(list);
            int partition_count = 0;
            for (ArrayList<HashMap<String, Integer>> partition : partition_list) {
                String floder_adress = Intermediate_File_floder + task_name + "/partition-" + partition_count++;
                writeObjectToFile(partition, floder_adress, this.getName());
            }
            return true;
        }catch(Exception e){
            sendMessage("error in"+this.getName()+" Mapping UDF has error :"+e.getMessage());
            return false;
        }
    }

    // invoke reduce UDF and write output to intermediate file
    private boolean reduce(){
        try {
            ArrayList<HashMap<String, Integer>> list = readPartition();
            if(list == null){return false;}
            HashMap<String, Integer> result = UDF.Reduce(list);
            if(writeObjectToFile(result,Intermediate_File_floder+task_name+"/reduce_output",this.getName())){
                return true;
            }
            return false;
        }catch(Exception e){
            sendMessage("error in"+this.getName()+"Reducing"+e.getMessage());
            return false;
        }
        
    }


    @Override
    public void run() {
        /**
         * ----------WOKER RULES----------
         * Woker start with sending a "Started" Message. Indicate Worker start succefully
         * Then base on the task master assign to woker, do different job(mapping. reducing, sorting)
         * When erro occur report erro to master and end the thread
         * When all jobs are done send "Done" to master and end the thread
         */

        // Send a message to let master know woker start successfully;
        sendMessage("Started ");  
        // fault tolerance test: Worker stop unexceptedly
        if(fault){
            Random r = new Random();
            int random_num = r.nextInt(100);
            if(random_num<chance_stop){
                return;
            }
        }
        // If this is a mapping task
        if(this.ismapper){
            String input = read_txt(this.input_fileSplit);
            if (input == null){
                sendMessage(this.getName()+"Worker fial to read input file");
                return;
            }
            // start mapping and when mapping fail send message
            if(!map(input)){
                sendMessage(this.getName()+"Worker Mapping fail");
                return;         
            }
        }   
        // If this is a sorting task
        else if (this.sorted){
            // start sorting and when sorting fail send message
            if(!sort_output()){
                sendMessage(this.getName()+"Worker Sorting fail");
                return; 
            }
        }   
        // this is a reducing task   
        else{
            // start sorting and when sorting fail send message
            if(!reduce()){
                sendMessage(this.getName()+"Worker Reducing fail");
                return;  
            }        
            
        }
        // Gladly send "Done" to woker indicate every thing is done
        sendMessage("Done");//(send Gladly)
        return;

    }

}
