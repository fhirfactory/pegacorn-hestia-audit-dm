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
package net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.audit;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.apache.camel.Header;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.fhirfactory.pegacorn.hestia.audit.dm.workshops.persistence.audit.common.AuditBaseProxy;

@ApplicationScoped
public class AuditSearchProxy extends AuditBaseProxy {
    private static final Logger LOG = LoggerFactory.getLogger(AuditSearchProxy.class);

    //
    // Business Methods
    //

    public List<String> doSearch(
            @Header("agentName") String agentName,
            @Header("entityType") String entityType,
            @Header("entityName") String entityName,
            @Header("date") String date,
            @Header("site") String site,
            @Header("limit") String limit) throws Throwable {
        getLogger().debug(".doSearch(): Entry, entityName->{}, date->{}, site->{}", entityName, date, site);
        List<String> answerList = new ArrayList<>();
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

        boolean agentNameExists = StringUtils.isNotEmpty(agentName);
        boolean entityTypeExists = StringUtils.isNotEmpty(entityType);
        boolean entityNameExists = StringUtils.isNotEmpty(entityName);
        boolean dateExists = StringUtils.isNotEmpty(date);
        boolean siteExists = StringUtils.isNotEmpty(site);
        boolean limitExists = StringUtils.isNotEmpty(limit);

        if(agentNameExists){
            filterList.addFilter(getAgentNameFilter(agentName));
        }
        if(entityTypeExists){
            filterList.addFilter(getEntityTypeFilter(entityType));
        }
        if(entityNameExists){
            filterList.addFilter(getEntityNameFilter(entityName));
        }
        if(dateExists){
            filterList.addFilter(getDateFilters(date));
        }
        if(siteExists){
            filterList.addFilter(getSiteFilter(site));
        }
        
        if (filterList.size() > 0) {
            if (limitExists) {
                try {
                    int lmt = Integer.parseInt(limit);
                    answerList = getResults(filterList, lmt, true);
                    getLogger().debug(".doSearch(): Exit. Limit search returning.");
                    return(answerList);
                } catch (NumberFormatException e) {
                    LOG.warn(".doSearch(): Invalid limit, number expecteed.");
                    return (answerList);
                }
            } else {
                answerList = getResults(filterList);
                getLogger().debug(".doSearch(): Exit. All records returning.");
                return(answerList);
            }
        }
        getLogger().debug(".doSearch(): Exit, no search done, invalid parameter set");
        return(answerList);
    }
    
    public Filter getAgentNameFilter(String agentName) {
       return new DependentColumnFilter(CF1, Q_AGENT_NAME, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(agentName)));
    }
    
    public Filter getEntityTypeFilter(String entityType) {
       return new DependentColumnFilter(CF1, Q_ENTITY_TYPE, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(entityType) + "$"));
    }

    public Filter getEntityNameFilter(String entityName) {
        return new DependentColumnFilter(CF1, Q_ENTITY_NAME, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(entityName) + "$"));
    }    
    
    public List<Filter> getDateFilters(String date)  throws Throwable {
        List<Filter> filters = new ArrayList<Filter>();
        Date startRange = parseDateString(date);
        Date endRange = parseEndRange(date);

        // StartDate needs to be less than end of range
        Filter startFilter = new DependentColumnFilter(CF1, Q_PERIOD_START, true, CompareOperator.LESS,
                new BinaryComparator(Bytes.toBytes(endRange.getTime())));
        // EndDate needs to be greater than start of range
        Filter endFilter = new DependentColumnFilter(CF1, Q_PERIOD_END, true, CompareOperator.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(startRange.getTime())));
        filters.add(startFilter);
        filters.add(endFilter);
        return filters;
    }  
    
    public Filter getSiteFilter(String site) {
        return new DependentColumnFilter(CF1, Q_SOURCE, true, CompareOperator.EQUAL, new RegexStringComparator("^" + prepareRegex(site) + "$"));
    }



    //
    // Getters (and Setters)
    //
    protected Logger getLogger(){
        return(LOG);
    }

}
