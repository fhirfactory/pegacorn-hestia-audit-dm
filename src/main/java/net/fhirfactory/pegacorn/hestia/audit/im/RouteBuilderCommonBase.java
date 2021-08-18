package net.fhirfactory.pegacorn.hestia.audit.im;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.slf4j.Logger;

/**
 * Common super class for all camel routes.
 * 
 */
public abstract class RouteBuilderCommonBase extends RouteBuilder {
    /**
     * All subclasses need to implement this method which defines all the routes.
     * All the routes may be skipped if these routes have been executed on another
     * cluster, instead just the result from the other cluster is used. If the last
     * route defined in this method does not have a "to"/"output" then the
     * distribution topic will be added as the "to"/"output" of that route Otherwise
     * an additional route will be added to send the result to all endpoints.
     */
    protected abstract void configureManagedRoutes();
    
    protected abstract Logger getLogger();

    /**
     * The intention is that in general this method should NOT be overridden by sub
     * classes. see {@link RouteBuilder#configure()}
     */
    @Override
    public void configure() {
        restConfiguration().component("servlet");

        configureManagedRoutes();

        logRoutes();
    }

    protected void logRoutes() {
        if (getLogger().isInfoEnabled()) {
            for (RouteDefinition route : getRouteCollection().getRoutes()) {
                getLogger().info("route=" + route);
            }
        }
    }
}
