/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw2;

/**
 *
 * @author hdadmin
 */
public class WorkerThread implements Runnable {
	 
	    public WorkerThread() {
	    }
	 
	    @Override
	    public void run() {
	        //System.out.println(Thread.currentThread().getName() + " Beginning job");
	        processCommand();
	        //System.out.println(Thread.currentThread().getName() + " Ending job");
	    }
	 
	    private void processCommand() {
	        try {
	            Thread.sleep(100);
	        } catch (InterruptedException e) {
	        }
	    }
}
