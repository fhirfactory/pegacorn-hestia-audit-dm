package net.fhirfactory.pegacorn.hestia.audit.servlet;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.archivers.zip.CharsetAccessor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fhirfactory.pegacorn.hestia.audit.dm.AuditEventProxy;

public class AuditEventWebServlet extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(AuditEventWebServlet.class);

    private static final long serialVersionUID = -1436416122014465914L;

    @Inject
    private AuditEventProxy auditProxy;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        List<NameValuePair> params = URLEncodedUtils.parse(request.getQueryString(), Charset.defaultCharset());

        for (NameValuePair param : params) {
            LOG.info(param.getName() + " : " + param.getValue());
        }

        int responseStatusCode = HttpServletResponse.SC_OK;
        String responseMsg = null;
        try {
            responseMsg = "Health Check Passed";
        } catch (Exception e) {
            LOG.error("Exception occurred performing health check", e);
            responseStatusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        }
        response.setStatus(responseStatusCode);
        if (responseMsg != null) {
            response.getWriter().write(responseMsg);
            response.getWriter().flush();
        }
    }

}
