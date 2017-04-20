# HHbase
custom HBase Hive and elasticsearch integration!

to use this project, you should have Hive 1.2 compatible Elasticsearch 2.3 which is https://github.com/ClayBrady6/Hive1.2compatibleElasticsearch2.3
and put these into Hive:
put this into HBase: 


How to code:

            NPBaseConfiguration config1 = new NPBaseConfiguration();
            config1.setValue(NPBaseConfiguration.HIVE_URL, "jdbc:hive2://node1:10000/default");
            config1.setValue(NPBaseConfiguration.HIVE_THRIFT_URL, "thrift://node1:9083");
            config1.setValue(NPBaseConfiguration.HIVEMYSQL_URL, "jdbc:mysql://node1:3306/hive");
            config1.setValue(NPBaseConfiguration.HIVEMYSQL_USER, "mysql");
            config1.setValue(NPBaseConfiguration.HIVEMYSQL_PASSWORD, "123456");
            config1.setValue(NPBaseConfiguration.HDFS_URL, "hdfs://node1:9000");
            config1.setValue(NPBaseConfiguration.MYCAT_SWITCH, "false");
            config1.setValue(NPBaseConfiguration.ZOOKEEPER_SERVER, "node1:2181,node2:2181");
            config1.setValue(NPBaseConfiguration.ESSWITCH, "true");
            config.setValue(NPBaseConfiguration.ES_ADDRESS, "node1,node2,node3");
            config1.setValue(NPBaseConfiguration.ES_PORT, "9300");
            config1.setValue(NPBaseConfiguration.ES_CLUSTER, "es_cluster");
            config1.setValue(NPBaseConfiguration.HBASE_HOST, "node1:60000");
            config1.setValue(NPBaseConfiguration.HBASE_ZOOKEEPERPORT, "2181");
            config1.setValue(NPBaseConfiguration.HBASE_ZOOKEEPERIP, "node1");
            config1.setValue(NPBaseConfiguration.USE_NPBASE_FILTER, "false");
            
            NPBaseEngine.build(config1);
            
            NPBaseEngine engine1 = NPBaseEngine.getSingleInstance();
            
so engine1 is basically where you call methods from.

have fun!
