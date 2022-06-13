package metaproxy

import (
	"encoding/json"
	"fmt"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"os"
	"time"

	"github.com/go-zookeeper/zk"
)

func getTableAddrInMetaProxy(client *executor.Client, zkAddr string, zkRoot string, tableName string) error {
	cluster, err := client.Meta.QueryClusterInfo()
	if err != nil {
		return err
	}

	if zkAddr == "" {
		zkAddr = cluster["zookeeper_hosts"]
	}
	zkConn, _, err := zk.Connect([]string{zkAddr}, time.Duration(1000*1000*1000))
	if err != nil {
		return err
	}
	defer zkConn.Close()

	currentRemoteZKInfo, err := ReadZkData(zkConn, zkRoot, tableName)
	if err != nil {
		return err
	}
	// formats into JSON
	outputBytes, _ := json.MarshalIndent(currentRemoteZKInfo, "", "  ")
	fmt.Fprintln(client, string(outputBytes))
	return nil
}

func addTableAddrInMetaProxy(client *executor.Client, zkAddr string, zkRoot string, tableName string) error {
	cluster, err := client.Meta.QueryClusterInfo()
	if err != nil {
		return err
	}

	if zkAddr == "" {
		zkAddr = cluster["zookeeper_hosts"]
	}
	zkConn, _, err := zk.Connect([]string{zkAddr}, time.Duration(1000*1000*1000))
	if err != nil {
		return err
	}
	defer zkConn.Close()

	clusterName := cluster["cluster_name"]
	clusterAddr := cluster["meta_servers"]
	_, _, err = WriteZkData(zkConn, zkRoot, tableName, clusterName, clusterAddr)
	if err != nil {
		return err
	}
	// formats into JSON
	tableInfo := MetaProxyTable{
		ClusterName: clusterName,
		MetaAddrs:   clusterAddr,
	}

	outputBytes, _ := json.MarshalIndent(tableInfo, "", "  ")
	fmt.Fprintln(client, string(outputBytes))
	return nil
}

func SwitchMetaAddrs(client *executor.Client, zkAddr string, zkRoot string, tableName string, targetAddrs string) error {
	cluster, err := client.Meta.QueryClusterInfo()
	if err != nil {
		return err
	}

	if zkAddr == "" {
		zkAddr = cluster["zookeeper_hosts"]
	}
	zkConn, _, err := zk.Connect([]string{zkAddr}, time.Duration(1000*1000*1000))
	if err != nil {
		return err
	}
	defer zkConn.Close()

	currentRemoteZKInfo, err := ReadZkData(zkConn, zkRoot, tableName)
	if err != nil {
		return err
	}

	currentLocalCluster := cluster["cluster_name"]
	if currentRemoteZKInfo.ClusterName != currentLocalCluster {
		return fmt.Errorf("current remote table is not `current local cluster`, remote vs expect= %s : %s",
			currentRemoteZKInfo.ClusterName, currentLocalCluster)
	}

	originMeta := client.Meta
	targetMeta := executor.NewClient(os.Stdout, []string{}).Meta
	env := map[string]string{
		"replica.deny_client_request": "reconfig*all",
	}

	targetCluster, err := targetMeta.QueryClusterInfo()
	if err != nil {
		return err
	}
	_, _, err = WriteZkData(zkConn, zkRoot, tableName, targetCluster["cluster_name"], targetAddrs)
	if err != nil {
		return err
	}

	err = originMeta.UpdateAppEnvs(tableName, env)
	if err != nil {
		return err
	}
	return nil
}

type MetaProxyTable struct {
	ClusterName string `json:"cluster_name"`
	MetaAddrs   string `json:"meta_addrs"`
}

func ReadZkData(zkConn *zk.Conn, root string, table string) (*MetaProxyTable, error) {
	tablePath := fmt.Sprintf("%s/%s", root, table)
	exist, _, _ := zkConn.Exists(tablePath)
	if !exist {
		return nil, fmt.Errorf("can't find the zk path: %s", tablePath)
	}

	data, _, err := zkConn.Get(tablePath)
	if err != nil {
		return nil, err
	}

	metaProxyTable := MetaProxyTable{}
	err = json.Unmarshal(data, &metaProxyTable)
	if err != nil {
		return nil, err
	}
	return &metaProxyTable, nil
}

func WriteZkData(zkConn *zk.Conn, root string, table string, cluster string, addrs string) (string, string, error) {
	zkData := encodeToZkNodeData(cluster, addrs)
	tablePath := fmt.Sprintf("%s/%s", root, table)
	exist, stat, _ := zkConn.Exists(tablePath)
	if !exist {
		_, err := zkConn.Create(tablePath, zkData, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return "", "", err
		}
	}
	_, err := zkConn.Set(tablePath, zkData, stat.Version)
	if err != nil {
		return "", "", err
	}

	return tablePath, string(zkData), nil
}

func encodeToZkNodeData(cluster string, addr string) []byte {
	data := fmt.Sprintf("{\"cluster_name\": \"%s\", \"meta_addrs\": \"%s\"}", cluster, addr)
	return []byte(data)
}
