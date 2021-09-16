/*
 * Copyright (c) 2021 Kelly Skye
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.edge.answer;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.rest.server.RestfulServer;
import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.AuditEventProxy;

//Class will need to be abstracted later when Aether wrapper is made
//For now, creating actual class for testing purposes
@ApplicationScoped
public class AuditDMAnswerServlet  extends RestfulServer {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDMAnswerServlet.class);

    
    private static final long serialVersionUID = 1L;

    
    @Inject
    private AuditEventProxy auditEventProxy;

    public AuditDMAnswerServlet() {
        LOG.info("AuditDMAnswerServlet initialised");
    }
    
}
