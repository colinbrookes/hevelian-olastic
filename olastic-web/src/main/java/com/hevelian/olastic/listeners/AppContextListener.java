package com.hevelian.olastic.listeners;

import java.net.UnknownHostException;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.hevelian.olastic.config.ESConfig;
import com.hevelian.olastic.config.ESConfigImpl;

import lombok.extern.log4j.Log4j2;

/**
 * Application context listener to get Elasticsearch properties from context and
 * initialize {@link ESConfig}.
 * 
 * @author rdidyk
 */
@Log4j2
public class AppContextListener implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        ServletContext ctx = sce.getServletContext();
        String host = ctx.getInitParameter("elastic.host");
        String port = ctx.getInitParameter("elastic.port");
        String cluster = ctx.getInitParameter("elastic.cluster");
        try {
            ESConfig config = new ESConfigImpl(host, Integer.valueOf(port), cluster);
            ctx.setAttribute(ESConfig.getName(), config);
        } catch (UnknownHostException e) {
            log.debug(e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        ServletContext ctx = sce.getServletContext();
        ESConfig config = (ESConfig) ctx.getAttribute(ESConfig.getName());
        config.close();
    }

}
