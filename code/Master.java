package code;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;


public class Master extends Thread {
    private static final String input_split = "code/input_split/";
    private static final String Intermediate_File_floder = "code/temp/";
    private String task_name;
    private int num_split;
    private MapReduceUDf UDF;
    private String Input_file;
    private int num_partition;
    private String output_file;

    // start master by variables(no longer use)
    public Master(MapReduceUDf UDF, String Input_file, int num_split, int num_partition, String task_name) {
        this.num_split = num_split;
        this.UDF = UDF;
        this.Input_file = Input_file;
        this.task_name = task_name;
        this.num_partition = num_partition;
    }

    // start master base on a configurationFile
    public Master(String configurationFile) {
        Properties configuration = new Properties();
        try {
            FileInputStream in = new FileInputStream(configurationFile);
            configuration.load(in);
            in.close();
            this.num_split = Integer.parseInt(configuration.getProperty("num_spilt"));
            this.num_partition = Integer.parseInt(configuration.getProperty("num_reduce"));
            this.Input_file = configuration.getProperty("input");
            this.task_name = configuration.getProperty("task_name");
            this.output_file = configuration.getProperty("output");
            // use reflect to get UDF
            Class<?> UDFclass = null;
            UDFclass = Class.forName(configuration.getProperty("UDF"));
            MapReduceUDf UDF = (MapReduceUDf) UDFclass.getDeclaredConstructor().newInstance();
            this.UDF = UDF;
        } catch (Exception e) {
            System.out.println(
                    "Master file fail to read configuration File, please check the configuration file and address is correct");
            e.printStackTrace();
        }

    }

    //read message from pipe
    private String read_message(Pipe pipe) {
        try {
            Pipe.SourceChannel sourceChannel = pipe.source();
            ByteBuffer buf = ByteBuffer.allocate(128);
            sourceChannel.read(buf); // read message to buffer
            buf.flip(); // set buffer to read 
            String message = "";
            while (buf.hasRemaining()) {
                message += (char) buf.get();
            }
            buf.clear();
            return message;
        } catch (IOException e) {
            System.out.println("can't get information from pipe");
            e.printStackTrace();
            return null;
        }

    }

    // Split Input file to N part base on user's need
    private String[] splitFile(String filePath, int fileCount) {
        try{
            String split_floder = input_split+ this.task_name  +"/";
            File floder = new File(split_floder);
            
            if (!floder.exists()) {
                floder.mkdirs();// creat a new floder
            }else{
                //delete old input split files
                deleteFolder(floder);
                floder.mkdirs();// creat a new floder
            }
            String[] split_files = new String[fileCount];
            String txt = "";
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists()) {
                long fileSize = file.length();
                long average = fileSize / fileCount;  // get the average file size of each split
                long filelength = 0;
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                int i = 0;
                // read in by line
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    txt = txt + lineTxt + " ";
                    filelength += lineTxt.getBytes(encoding).length;
                    // the content is enough for one split
                    if (filelength >= average){
                        split_files[i]= split_floder +  i + ".txt";                    
                        toTxtFile(txt,split_files[i]);
                        i+=1;
                        filelength = 0;
                        txt = "";                   
                    }
                }
                // write the rest to one split
                split_files[i]= input_split + this.task_name +"/" + i + ".txt";                    
                toTxtFile(txt,split_files[i]);
                read.close();
                // delete old intermediate files
                floder = new File(Intermediate_File_floder+this.task_name+"/");
                deleteFolder(floder);
                return split_files;
            } else {
                System.out.println("File not found.");
                return null;
            }
        } catch (Exception e) {
            System.out.println("Read in fail");
            e.printStackTrace();
            return null;
        }
        
    }
    
    // write string to .txt file
    private boolean toTxtFile(String value, String filename) {
        String encoding = "UTF-8";
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename),encoding)));
            out.write(value);
            out.close();
        } catch (Exception e) {
        }
        return true;
    }
    
    // delete a floder and all files and floder below it
    public static void deleteFolder(File folder) throws Exception {
        if (!folder.exists()) {
            return;
        }
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // recursion delete floder below it
                    deleteFolder(file);
                } else {
                    // delete files
                    file.delete();
                }
            }
        }
        // every thing below is delete, now delete the floder
        folder.delete();
    }

    // Start N worker to finish mapping task
    private ArrayList<WorkerInfo> startNewMappers(String[] Input_fileSplits) {
        ArrayList<WorkerInfo> workers_list = new ArrayList<WorkerInfo>();
        try {
            // Assign Mappings jobs to Workers for each input Split
            for (String file : Input_fileSplits) {
                Pipe pipe = Pipe.open();
                Worker newWorker = new Worker(UDF, file, pipe, this.task_name, this.num_partition, true);               
                // store worker's information
                WorkerInfo info = new WorkerInfo(newWorker, pipe, file, true,0);
                workers_list.add(info);
                newWorker.start();
            }
            return workers_list;
        } catch (IOException e_map) {
            System.out.println("Error in getting connnect with Worker");
            e_map.printStackTrace();
        }
        return null;
    }

    // start M workers to finish Reduce task
    private ArrayList<WorkerInfo> startNewReducer() {
        ArrayList<WorkerInfo> reducer_list = new ArrayList<WorkerInfo>();
        try {
            for (int i = 0; i < this.num_partition; i++) {
                Pipe pipe = Pipe.open();
                Worker new_reducer = new Worker(UDF, pipe, this.task_name, i, false);
                // store worker's information
                WorkerInfo info = new WorkerInfo(new_reducer, pipe, this.task_name, i, false,0);
                reducer_list.add(info);
                new_reducer.start();
            }
        } catch (IOException e_map) {
            System.out.println("Error in getting connnect with Reducer");
            e_map.printStackTrace();
            return null;
        }
        return reducer_list;
    }

    // start 1 workers to finish sorting task
    private ArrayList<WorkerInfo> sort_output() {
        ArrayList<WorkerInfo> reducer_list = new ArrayList<WorkerInfo>();
        try {
            Pipe pipe = Pipe.open();
            Worker sort_worker = new Worker(pipe, task_name, output_file, true);
            WorkerInfo info = new WorkerInfo(sort_worker, pipe, task_name,0, true);
            reducer_list.add(info);
            sort_worker.start();
            return reducer_list;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Check State of Worker, also receive and process messages from worker
    private boolean Moniter_Worker(ArrayList<WorkerInfo> worker_list) {
        HashMap<Integer, WorkerInfo> worker_running = new HashMap<Integer, WorkerInfo>();
        int worker_num = 0;
        for (WorkerInfo info : worker_list) {
            worker_running.put(worker_num++, info);
        }

        while (!worker_running.isEmpty()) {
            // check if each worker is working every second. End when all worker is done
            System.out.println("------ Start checking Worker statue ------  ");
            ArrayList<Integer> keys = new ArrayList<Integer>(worker_running.keySet());
            for (int i = 0; i < keys.size(); i++) {
                // get worker's Information
                int key = keys.get(i);
                WorkerInfo info = worker_running.get(key);
                Worker running_maper = info.worker;
                Pipe pipe = info.pipe;
                // check the statu of worker if it is TERMINATED
                State state = running_maper.getState();
                if (state != Thread.State.TERMINATED) {
                    // Worker is Working do nothing
                    System.out.println(" Worker: " +running_maper.getName()+" | State: " +  running_maper.getState());
                } else {
                    // Worker is not Working, check why
                    String message = read_message(pipe);
                    // get condition type base on worker's message
                    int condition = message_process(message);
                    switch (condition) {
                        // fault tolerance
                        case 0: // worker end Unexcepted, reassign task
                            System.out.println("!!! Worker: "+ running_maper.getName()+" Unexpected ended !!!");
                            WorkerInfo new_info = reassign_Worker(info);
                            if (new_info != null) {
                                worker_running.put(worker_num++, new_info);
                                System.out.println("Master has restart the task assigned to " + new_info.worker.getName());
                            } else {
                                System.out.println("Fail to restart the task");
                                return false;
                            }
                            break;
                        case 1: // worker has finish their job and End correctly
                            System.out.println(" Worker: "+ running_maper.getName()+" Finish successfully");
                            break;
                        case 2: // There is an error that worker cannot fix. End the task
                            System.out.println(message);
                            System.out.println("!!! Worker: "+ running_maper.getName()+"State: Error occur !!!");
                            System.out.println("Worker can't fix this error. Task stop");
                            return false;
                        default:
                            return false;
                    }
                    // remove the worker from the moniter's list
                    worker_running.remove(key);
                }
            }
            // check every second
            System.out.println("--------End of this check--------");
            pauseThisThread(500);
        }
        System.out.println("all workers done their job");
        return true;
    }

    // pause This Thread for a while, InterruptedException fixed
    private void pauseThisThread(int m) {
        try {
            this.sleep(m);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // reassign the task to another worker
    private WorkerInfo reassign_Worker(WorkerInfo info) {
        if (info.repeat_count >= 5) {
            System.out.println("Task has been try with 5 driver, stop retarting");
            return null;
        }
        try {
            Pipe new_pipe = Pipe.open();
            WorkerInfo new_info = null;
            if (info.ismapper) {
                Worker newWorker = new Worker(UDF, info.file_path, new_pipe, this.task_name, this.num_partition, true);
                new_info = new WorkerInfo(newWorker, new_pipe, info.file_path, true, ++info.repeat_count);
            } else if (info.sorted) {
                Worker sort_worker = new Worker(new_pipe, task_name, output_file, true);
                new_info = new WorkerInfo(sort_worker, new_pipe, task_name, ++info.repeat_count, true);

            } else if (!info.ismapper) {
                Worker new_reducer = new Worker(UDF, new_pipe, this.task_name, info.partition, false);
                new_info = new WorkerInfo(new_reducer, new_pipe, this.task_name, info.partition, false,
                        ++info.repeat_count);
            } else {
                return null;
            }
            new_info.worker.start();
            return new_info;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // process message send from worker
    private int message_process(String message) {
        if (message == null) {
            return 0;
        }
        // thread started successfully but end in an unexpected condition,
        // no extra message return, e.g. a sudden death
        if (message.equals("Started ")) {
            return 0;
        }
        // works start and finished job successfully
        if (message.equals("Started Done")) {
            return 1;
        }
        // works occur an erro or fail to do the task
        if (message.contains("error")||message.contains("fail")) {
            return 2;
        }
        return 0;
    }

    @Override
    public void run() {
        /**
         * ----------WOKER RULES----------
         * Spilt input files to N part
         * Start workers, Moniter their behavior and process their messages
         * Start reduce task only when all mapping task is finished
         */

        // File splite part
        String[] Input_fileSplits = splitFile(this.Input_file, this.num_split);
        //Input_fileSplits = new String[1];
        //Input_fileSplits[0]="dataset/hamlet.txt";
        if (Input_fileSplits == null) {
            return;
        }
        // mapper staring part
        System.out.println("--------Start of mapping--------");
        ArrayList<WorkerInfo> worker_list = startNewMappers(Input_fileSplits);
        if (worker_list == null) {
            return;
        }
        // Mapper moniter part
        if (!Moniter_Worker(worker_list)) {
            return;
        }
        System.out.println("---------End of mapping---------");
        System.out.println("---------------------------------");
        // reducer staring part
        System.out.println("--------Start of reducing--------");
        worker_list = startNewReducer();
        if (worker_list == null) {
            return;
        }
        if (!Moniter_Worker(worker_list)) {
            return;
        }
        System.out.println("--------End of reducing--------");
        // Start of sorting output and merge to one file
        System.out.println("--------Start of sorting--------");
        worker_list = sort_output();
        if (worker_list.isEmpty()) {
            return;
        }
        if (!Moniter_Worker(worker_list)) {
            return;
        }
        System.out.println("--------End of sorting--------");

    }

}