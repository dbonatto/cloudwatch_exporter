package io.prometheus.cloudwatch;

import io.prometheus.client.exporter.MetricsServlet;
import java.io.FileReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


public class WebServer {

  private static final Logger LOGGER = Logger.getLogger(CollectorWorker.class.getName());

  public static void main(String[] args) throws Throwable {
     if (args.length < 2) {
       System.err.println("Usage: WebServer <port> <yml configuration file>");
       System.exit(1);
     }
     CloudWatchCollector cc = new CloudWatchCollector(new FileReader(args[1])).register();

     Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
       public void uncaughtException(Thread t, Throwable e) {
         System.err.println(t + " throws exception: " + e);
       }
     });

     try {
       int port = Integer.parseInt(args[0]);
       Server server = new Server(port);
       ServletContextHandler context = new ServletContextHandler();
       context.setContextPath("/");
       server.setHandler(context);
       context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
       context.addServlet(new ServletHolder(new HomePageServlet()), "/");
       server.start();
       server.join();
     } catch (Throwable e) {
        LOGGER.log(Level.SEVERE, "Error while running the webserver.", e);
        throw e;
     }
   }
}
