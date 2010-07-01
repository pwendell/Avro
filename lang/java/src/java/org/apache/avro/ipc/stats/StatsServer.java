package org.apache.avro.ipc.stats;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.resource.FileResource;
import org.mortbay.resource.Resource;

/* This is a server that displays live information from a StatsPlugin.
 * 
 *  Typical usage is as follows:
 *    StatsPlugin plugin = new StatsPlugin(); 
 *    requestor.addPlugin(plugin);
 *    StatsServer server = new StatsServer(plugin, 8080);
 *    
 *  */
public class StatsServer {
  Server httpServer;
  StatsPlugin plugin;
  
  /* Start a stats server on the given port, 
   * responsible for the given plugin. */
  public StatsServer(StatsPlugin plugin, int port) throws Exception {
    this.httpServer = new Server(port);
    this.plugin = plugin;
    
    // This servlet is responsible for static content
    Context staticContext = new Context(httpServer, "/static");
    ServletHolder staticHolder = new ServletHolder(new StaticServlet());
    staticContext.addServlet(staticHolder, "/*");
    
    // For dynamic content...
    Context context = new Context(httpServer, "/");
    context.addServlet(new ServletHolder(new StatsServlet(plugin)), "/*");
    
    httpServer.start();
  }
  
  /* Stops this server. */
  public void stop() throws Exception {
    this.httpServer.stop();
  }
  
  private class StaticServlet extends DefaultServlet {
    public Resource getResource(String pathInContext) {
      // Take only last slice of the URL as a filename, so we can adjust path. 
      // This also prevents mischief like '../../foo.css'
      String[] parts = pathInContext.split("/");
      String filename =  parts[parts.length - 1];
      try {
        URL resource = getClass().getClassLoader().getResource(
            "org/apache/avro/ipc/stats/templates/" + filename);
        if (resource == null) { return null; }
        return new FileResource(resource);
      } catch (IOException e) {
        return null;
      } catch (URISyntaxException e) {
        return null;
      }
    }
  }
}
