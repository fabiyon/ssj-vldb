/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.utils;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 *
 * @author fabi
 */
public class TokenizeEnronWorker extends Thread {

  private static int instance = 0;
  private final ArrayBlockingQueue<TokenizeEnron> queue;

  public TokenizeEnronWorker(ArrayBlockingQueue queue) {
    this.queue = queue;
    setName("MyWorker:" + (instance++));
  }

  @Override
  public void run() {
    while (true) {
      try {
        Runnable work = null;

        synchronized (queue) {
          while (queue.isEmpty()) {
            queue.wait();
          }

          // Get the next work item off of the queue
          work = queue.remove();
        }

        // Process the work item
        work.run();
      } catch (InterruptedException ie) {
        break;  // Terminate
      }
    }
  }

//  private void doWork(Runnable work) {...
//  }
}
