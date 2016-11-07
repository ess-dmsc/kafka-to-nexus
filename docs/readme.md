# Kafka to Nexus file writing

Documents:

[Nexus-for-ESS](https://ess-ics.atlassian.net/wiki/display/DMSC/NeXus+for+ESS)

[file-writer-2016-10-28](https://ess-ics.atlassian.net/wiki/download/attachments/48202445/BrightNeXus.pdf?version=1&modificationDate=1477659873237&cacheVersion=1&api=v2)


## Graph of dependencies and data flow

Very early draft so far:

[Graphviz dot](flow.gv)

[Graphviz png](flow.png)


## To be discussed

- Latency requirements (max/avg), probably governed by buffer capacity on the Kafka broker
  - Process a new configuration and open new file
  - Close current file and open a new one (buffer capacity)




## Ideas for the future

### Data storage
- Storage
  - [BeeGFS](http://www.beegfs.com/content/)
    <http://www.itwm.fraunhofer.de/en/fraunhofer-itwm.html>
  - [Hadoop](http://hadoop.apache.org/)
  - [Pithos](https://www.exoscale.ch/syslog/2016/08/15/object-storage-cassandra-pithos/)
    <http://pithos.io/>
    <https://github.com/exoscale/pithos>
