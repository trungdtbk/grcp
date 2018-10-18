graph-based Routing Control Platform (gRCP)
==========================================

A routing control platform (RCP) for BGP routing management. gRCP uses a graph database
for storing routing states and traffic statistics. Applications can make API calls to
query network states and to program a router's FIB table. 

With gRCP BGP route selection is similar to a graph query rather than the conventional BGP ranking algorithm.

gRCP does not work with a normal BGP router. It requires a special BGP implementation based on Faucet controller.
More information will be added soon.
