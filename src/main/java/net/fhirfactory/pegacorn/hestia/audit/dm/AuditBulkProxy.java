package net.fhirfactory.pegacorn.hestia.audit.dm;

import org.hl7.fhir.instance.model.api.IDomainResource;

public class AuditBulkProxy extends AuditBaseProxy {


    @Override
    protected StoreAuditOutcomeEnum saveToDatabase(IDomainResource resouce) throws Exception {
        // TODO Auto-generated method stub
        return StoreAuditOutcomeEnum.FAILED;
    }

}
