FROM fhirfactory/pegacorn-base-docker-wildfly:1.0.0

# ENV KEYCLOAK_VERSION 11.0.2

# USER jboss

# # Copy and then extract the Keycloak adapter files
# # Sourced from https://www.keycloak.org/downloads.html
# COPY authorisation-server/keycloak-wildfly-adapter-dist-$KEYCLOAK_VERSION.tar.gz $JBOSS_HOME/keycloak-wildfly-adapter-dist-$KEYCLOAK_VERSION.tar.gz
# RUN tar -xvf $JBOSS_HOME/keycloak-wildfly-adapter-dist-$KEYCLOAK_VERSION.tar.gz -C $JBOSS_HOME
# # Apply the customisation to https://github.com/keycloak/keycloak/blob/master/core/src/main/java/org/keycloak/TokenVerifier.java (compare with authorisation-server/TokenVerifier.java) until Pull Request is done
# COPY authorisation-server/keycloak-core-$KEYCLOAK_VERSION-custom.jar $JBOSS_HOME/modules/system/add-ons/keycloak/org/keycloak/keycloak-core/main/keycloak-core-$KEYCLOAK_VERSION.jar

# Run cli to install the adapter.  The above extracted some cli files to $JBOSS_HOME/bin
# RUN $JBOSS_HOME/bin/jboss-cli.sh --file=$JBOSS_HOME/bin/adapter-elytron-install-offline.cli && \
#     rm -rf $JBOSS_HOME/standalone/configuration/standalone_xml_history/current/*

# # Copy and run cli to modify the standalone.xml configuration.
# COPY authorisation-server/cli/keycloak-adapter-configuration.cli $JBOSS_HOME/bin/keycloak-adapter-configuration.cli

# Replace the default wildfly welcome content page for the URL /, with a blank html page, so the application server is not easily exposed to callers.
RUN mv $JBOSS_HOME/welcome-content/index.html $JBOSS_HOME/welcome-content/index-bak.html
COPY /src/main/webapp/index.html $JBOSS_HOME/welcome-content/index.html

# deploy the application
COPY target/*.war $JBOSS_HOME/standalone/deployments/

COPY setup-env-then-start-wildfly-as-jboss.sh /
COPY start-wildfly.sh /

ARG IMAGE_BUILD_TIMESTAMP
ENV IMAGE_BUILD_TIMESTAMP=${IMAGE_BUILD_TIMESTAMP}
RUN echo IMAGE_BUILD_TIMESTAMP=${IMAGE_BUILD_TIMESTAMP}

USER root
# Install gosu based on
# 1. https://gist.github.com/rafaeltuelho/6b29827a9337f06160a9
# 2. https://github.com/tianon/gosu
# 3. https://github.com/tianon/gosu/releases/download/1.12/gosu-amd64
COPY gosu-amd64 /usr/local/bin/gosu
RUN chmod +x /usr/local/bin/gosu && \
	chmod +x /setup-env-then-start-wildfly-as-jboss.sh && \
   	chmod +x /start-wildfly.sh

CMD	["/setup-env-then-start-wildfly-as-jboss.sh"]
