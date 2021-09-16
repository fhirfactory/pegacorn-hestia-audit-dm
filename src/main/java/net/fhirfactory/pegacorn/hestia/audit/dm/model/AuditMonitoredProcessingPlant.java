/*
 * Copyright (c) 2021 Kelly Skye (ACT Health)
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
package net.fhirfactory.pegacorn.hestia.audit.dm.model;

import java.util.ArrayList;
import java.util.List;

import net.fhirfactory.pegacorn.hestia.audit.dm.model.common.AuditMonitoredNode;

public class AuditMonitoredProcessingPlant extends AuditMonitoredNode {
    private String platformID;
    private String securityZone;
    private String site;
    private List<AuditMonitoredWorkshop> workshops;
    private String actualHostIP;
    private String actualPodIP;

    public AuditMonitoredProcessingPlant(){
        workshops = new ArrayList<>();
    }

    public String getPlatformID() {
        return platformID;
    }

    public void setPlatformID(String platformID) {
        this.platformID = platformID;
    }

    public List<AuditMonitoredWorkshop> getWorkshops() {
        return workshops;
    }

    public void setWorkshops(List<AuditMonitoredWorkshop> workshops) {
        this.workshops = workshops;
    }

    public String getSecurityZone() {
        return securityZone;
    }

    public void setSecurityZone(String securityZone) {
        this.securityZone = securityZone;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getActualHostIP() {
        return actualHostIP;
    }

    public void setActualHostIP(String actualHostIP) {
        this.actualHostIP = actualHostIP;
    }

    public String getActualPodIP() {
        return actualPodIP;
    }

    public void setActualPodIP(String actualPodIP) {
        this.actualPodIP = actualPodIP;
    }

    public String toString() {
        return "AuditMonitoredProcessingPlant{" +
                "platformID='" + platformID + '\'' +
                ", workshops=" + workshops +
                ", nodeName='" + getNodeID() + '\'' +
                ", nodeVersion='" + getNodeVersion() + '\'' +
                ", nodeType=" + getNodeType() +
                ", concurrencyMode='" + getConcurrencyMode() + '\'' +
                ", resilienceMode='" + getResilienceMode() + '\'' +
                '}';
    }
}
