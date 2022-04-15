
import code.Master;
import UDF.UDF_Wordcount;
import UDF.UDF_Wordlength;
import UDF.UDF_movie;
public class test extends Thread{

    private void pauseThisThread(int m) {
        try {
            this.sleep(m);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void run(){
        Master master = null;
        System.out.println("Start task hamlet_WordCount.properties");
        master = new Master("hamlet_WordCount.properties");
        master.start();
        pauseThisThread(10000);
        System.out.println("Start task Movie_genercount.properties");
        master = new Master("Movie_genercount.properties");
        master.start();
        pauseThisThread(10000);
        System.out.println("Start task hamlet_wordlenth.properties");
        master = new Master("hamlet_wordlenth.properties");
        master.start();
    }

    public static void main(String[] args){
        test t =new test();
        t.run();      
    }
}