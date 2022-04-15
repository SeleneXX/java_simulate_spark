package code;

import java.nio.channels.Pipe;

public class WorkerInfo {
    /**
     * A Structure to store Worker's information. Good for code reading. 
     * No function just for storing datas
     */

    public Worker worker; // pointer to worker
    public MapReduceUDf UDF; // UDF that assign to worker
    public String file_path; // File Split or partition assigned to worker
    public Boolean ismapper; // Mapper or Reducer
    public Pipe pipe; // Pipe for comunication to Master
    public int repeat_count; // how many times this task has been rerun;
    public boolean sorted;
    public int partition;

    // if woker is started second time

    WorkerInfo(Worker worker, Pipe pipe, String file_path, Boolean ismapper,int repeat_count) {
        // for new mapper times this task has been rerun is 0
        this.worker = worker;
        this.pipe = pipe;
        this.file_path = file_path;
        this.ismapper = ismapper;
        this.repeat_count = repeat_count;
        this.sorted =false;
    }
    WorkerInfo(Worker worker, Pipe pipe,  String task_name,int partition,Boolean ismapper,int repeat_count) {
        //Use given message to build a reducer
        this.worker = worker;
        this.pipe = pipe;
        this.ismapper = ismapper;
        this.repeat_count = repeat_count;
        this.partition = partition;
        this.sorted =false;

    }
    WorkerInfo(Worker worker, Pipe pipe,String task_name,int repeat_count,boolean sorted){
        //Use given message to build a sorting
        this.worker = worker;
        this.pipe = pipe;
        this.sorted = sorted;
        this.repeat_count = repeat_count;
        this.ismapper = false;
    }

}
