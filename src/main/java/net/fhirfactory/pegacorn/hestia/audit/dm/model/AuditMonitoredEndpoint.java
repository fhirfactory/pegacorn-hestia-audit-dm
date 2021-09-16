/*
 * Copyright (c) 2021 Mark A. Hunter (ACT Health)
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

import java.util.Objects;

import net.fhirfactory.pegacorn.deployment.topology.model.endpoints.common.PetasosEndpointTopologyTypeEnum;
import net.fhirfactory.pegacorn.hestia.audit.dm.model.common.AuditMonitoredNode;

public class AuditMonitoredEndpoint extends AuditMonitoredNode {
    private String localDNSEntry;
    private String localPort;
    private String localBasePath;
    private String localServicePort;
    private String localServiceDNSEntry;
    private String actualHostIP;
    private String actualPodIP;
    private String remoteDNSEntry;
    private String remotePort;
    private String remoteBasePath;
    private String remoteSystemName;
    private PetasosEndpointTopologyTypeEnum endpointType;
    private boolean encrypted;

    public String getLocalDNSEntry() {
        return localDNSEntry;
    }

    public void setLocalDNSEntry(String localDNSEntry) {
        this.localDNSEntry = localDNSEntry;
    }

    public String getLocalPort() {
        return localPort;
    }

    public void setLocalPort(String localPort) {
        this.localPort = localPort;
    }

    public String getLocalBasePath() {
        return localBasePath;
    }

    public void setLocalBasePath(String localBasePath) {
        this.localBasePath = localBasePath;
    }

    public String getRemoteDNSEntry() {
        return remoteDNSEntry;
    }

    public void setRemoteDNSEntry(String remoteDNSEntry) {
        this.remoteDNSEntry = remoteDNSEntry;
    }

    public String getRemotePort() {
        return remotePort;
    }

    public void setRemotePort(String remotePort) {
        this.remotePort = remotePort;
    }

    public String getRemoteBasePath() {
        return remoteBasePath;
    }

    public void setRemoteBasePath(String remoteBasePath) {
        this.remoteBasePath = remoteBasePath;
    }

    public String getRemoteSystemName() {
        return remoteSystemName;
    }

    public void setRemoteSystemName(String remoteSystemName) {
        this.remoteSystemName = remoteSystemName;
    }

    public PetasosEndpointTopologyTypeEnum getEndpointType() {
        return endpointType;
    }

    public void setEndpointType(PetasosEndpointTopologyTypeEnum endpointType) {
        this.endpointType = endpointType;
    }

    public boolean isEncrypted() {
        return encrypted;
    }

    public void setEncrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }

    public String getLocalServicePort() {
        return localServicePort;
    }

    public void setLocalServicePort(String localServicePort) {
        this.localServicePort = localServicePort;
    }

    public String getLocalServiceDNSEntry() {
        return localServiceDNSEntry;
    }

    public void setLocalServiceDNSEntry(String localServiceDNSEntry) {
        this.localServiceDNSEntry = localServiceDNSEntry;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AuditMonitoredEndpoint)) return false;
        AuditMonitoredEndpoint that = (AuditMonitoredEndpoint) o;
        return isEncrypted() == that.isEncrypted() && Objects.equals(getLocalDNSEntry(), that.getLocalDNSEntry()) && Objects.equals(getLocalPort(), that.getLocalPort()) && Objects.equals(getLocalBasePath(), that.getLocalBasePath()) && Objects.equals(getLocalServicePort(), that.getLocalServicePort()) && Objects.equals(getLocalServiceDNSEntry(), that.getLocalServiceDNSEntry()) && Objects.equals(getActualHostIP(), that.getActualHostIP()) && Objects.equals(getActualPodIP(), that.getActualPodIP()) && Objects.equals(getRemoteDNSEntry(), that.getRemoteDNSEntry()) && Objects.equals(getRemotePort(), that.getRemotePort()) && Objects.equals(getRemoteBasePath(), that.getRemoteBasePath()) && Objects.equals(getRemoteSystemName(), that.getRemoteSystemName()) && getEndpointType() == that.getEndpointType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLocalDNSEntry(), getLocalPort(), getLocalBasePath(), getLocalServicePort(), getLocalServiceDNSEntry(), getActualHostIP(), getActualPodIP(), getRemoteDNSEntry(), getRemotePort(), getRemoteBasePath(), getRemoteSystemName(), getEndpointType(), isEncrypted());
    }

    @Override
    public String toString() {
        return "AuditMonitoredEndpoint{" +
                "localDNSEntry='" + localDNSEntry + '\'' +
                ", localPort='" + localPort + '\'' +
                ", localBasePath='" + localBasePath + '\'' +
                ", localServicePort='" + localServicePort + '\'' +
                ", localServiceDNSEntry='" + localServiceDNSEntry + '\'' +
                ", actualHostIP='" + actualHostIP + '\'' +
                ", actualPodIP='" + actualPodIP + '\'' +
                ", remoteDNSEntry='" + remoteDNSEntry + '\'' +
                ", remotePort='" + remotePort + '\'' +
                ", remoteBasePath='" + remoteBasePath + '\'' +
                ", remoteSystemName='" + remoteSystemName + '\'' +
                ", endpointType=" + endpointType +
                ", encrypted=" + encrypted +
                ", nodeName='" + getNodeID() + '\'' +
                ", nodeVersion='" + getNodeVersion() + '\'' +
                ", nodeType=" + getNodeType() +
                ", concurrencyMode='" + getConcurrencyMode() + '\'' +
                ", resilienceMode='" + getResilienceMode() + '\'' +
                ", componentName='" + getComponentName() + '\'' +
                '}';
    }
}
