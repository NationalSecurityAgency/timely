# Developer Information

## Building Timely

Timely requires a Java 8 and uses Maven for compiling source, running tests, and packaging distributions. Below is a set of useful Maven lifecyles that we use:
 
Command | Description
--------|------------
mvn compile | Compiles and formats the source
mvn test | Runs unit tests
mvn package | Runs findbugs and creates a distribution
mvn verify | Runs integration tests
mvn verify site | Creates the site

> [CollectD] (http://collectd.org/)

<aside class="warning">
The CollectD plugins require that CollectD is installed locally as the `/usr/share/collectd/java/collectd-api.jar` file is a dependency.
</aside>

> [os-maven-plugin] (https://github.com/trustin/os-maven-plugin#issues-with-eclipse-m2e-or-other-ides)

<aside class="warning">
If you are having trouble with the server pom in your IDE, take a look at the os-maven-plugin page.
</aside>

> [Netty-TCNative] (http://netty.io/wiki/forked-tomcat-native.html)

<aside class="warning">
Timely uses the netty-tcnative module to provide native access to OpenSSL. The correct version of the artifact is downloaded during the build. If you are building for a different platform, then you can override the classifier by specifying the `os.detected.classifier` property on the Maven command line. See the netty-tcnative wiki for more information.
</aside>

## Netty Pipeline Design

Timely supports multiple protocols, each having their own Netty pipeline.
Each pipeline is set up in Server.java and includes Netty transport specific
handlers, followed by a Decoder (TcpDecoder, HttpRequestDecoder,
WebSocketRequestDecoder) that transforms the input request to a Java object.
Handlers for each request Java object then follow and will perform request type
specific actions and may or may not emit a response to the client in the format
required by the transport (bytes for TCP, HTTP message for HTTP, web socket frame
for WebSocket).

A single request type (e.g. VersionRequest) can be used across different protocols.
Annotations for the different transports are used in the different decoders to match
a request type to an operation or path. For example "version" is matched to a VersionRequest
in the TCP protocol and it's matched to "/version" in the HTTP protocol. Request
objects will typically also implement an interface for each protocol for which they
have a corresponding annotation. The interface methods are called from the protocol
decoder to deserialize the message sent across the wire to a Java object.

Adding a new operation / request object:

1. Create the object in the timely.api.request package and apply appropriate transport annotations
2. Add a Handler for your request type in the appropriate transport channels
3. Add tests for each transport decoder to ensure that your object is deserialized properly
4. Add test cases to the appropriate integration tests
5. Update the README api section.

