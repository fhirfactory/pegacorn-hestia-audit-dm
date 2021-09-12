package net.fhirfactory.pegacorn.hestia.audit.servlet;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fhirfactory.pegacorn.hestia.audit.dm.AuditSearchProxy;

@WebServlet()
public class AuditEventWebServlet extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(AuditEventWebServlet.class);

    private static final long serialVersionUID = -1436416122014465914L;

    @Inject
    private AuditSearchProxy auditProxy;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        LOG.info("Get called");
        List<NameValuePair> params = URLEncodedUtils.parse(request.getQueryString(), Charset.defaultCharset());

        String entityType = null, date = null, agentName = null, entityName = null, site = null;
        for (NameValuePair param : params) {
            if("entity-type".equals(param.getName())) {
                entityType = param.getValue();
            }
            if("date".equals(param.getName())) {
                date = param.getValue();
            }
            if("agent-name".equals(param.getName())) {
                agentName = param.getValue();
            }
            if("entity-name".equals(param.getName())) {
                entityName = param.getValue();
            }
            if("site".equals(param.getName())) {
                site = param.getValue();
            }
            LOG.info(param.getName() + " : " + param.getValue());
        }

        int responseStatusCode = HttpServletResponse.SC_OK;
        String responseMsg = null;
        try {
            if(StringUtils.isNotBlank(agentName)) {
                responseMsg = parseResults(auditProxy.getByUser(agentName));
            } else if (StringUtils.isNotBlank(entityType) && StringUtils.isNotBlank(date)) {
                responseMsg = parseResults(auditProxy.getByTypeAndDate(entityType, date));
            } else if (StringUtils.isNotBlank(site) && StringUtils.isNotBlank(entityName) && StringUtils.isNotBlank(date)) {
                responseMsg = parseResults(auditProxy.getBySiteNameAndDate(site, entityName, date));
            } else {
                responseMsg = "Invalid Parameters";
            }
        } catch (Exception e) {
            LOG.error("Exception occurred performing search", e);
            responseStatusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        } catch (Throwable e) {
            LOG.error("Date unable to be parsed: " + e.getMessage());
            responseMsg = "Invalid date passed";
        }
        response.setStatus(responseStatusCode);
        if (responseMsg != null) {
            response.getWriter().write(responseMsg);
            response.getWriter().flush();
        }
    }
    
    private String parseResults(List<String> values) {
        if(values == null || values.size() == 0) {
            return "No results found";
        }
        StringBuilder sb = new StringBuilder();
        for(String value : values) {
            sb.append(value);
            sb.append(',');
        }
        return sb.substring(0, sb.length() -1);
    }

}
