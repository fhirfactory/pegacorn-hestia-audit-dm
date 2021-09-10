::mvn clean install -DskipTests
docker pull fhirfactory/pegacorn-base-docker-wildfly:1.0.0
docker build --rm --build-arg IMAGE_BUILD_TIMESTAMP="%date% %time%" -t pegacorn-hestia-audit-dm:1.0.0-snapshot --file Dockerfile .
helm upgrade pegacorn-hestia-audit-dm-site-a --install --namespace site-a --set serviceName=pegacorn-hestia-audit-dm,imagePullPolicy=Never,basePort=32510,basePortInsidePod=8443,hostPathCerts=/data/certificates,imageTag=1.0.0-snapshot,jvmMaxHeapSizeMB=768,wildflyLogLevel=INFO,javaxNetDebug=none,wildflyAdminUser=admin,wildflyAdminPwd=Pega@dm1n,numOfPods=1 helm