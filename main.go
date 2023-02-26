package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var wg sync.WaitGroup
var masterKey = false
var mutex = &sync.Mutex{}
var dividedWork [][]string

type NodeInfo struct {
	NodeId     int    `json:"nodeId"`
	NodeIpAddr string `json:"nodeIpAddr"`
	Port       string `json:"port"`
	Status     string `json:"status"`
}

var nodes = make(map[NodeInfo]string)
var masterNode NodeInfo

// A Request/Response format to transfer between nodes
// `Message` is the sorted/unsorted slice
type data struct {
	Source  NodeInfo
	Dest    NodeInfo
	Message []string
}

func main() {
	numberOfNodes := flag.Int("numberOfNodes", 3, "number of slaves to use")
	clusterIp := flag.String("clusterIp", "127.0.0.1", "ip address of slave node")
	port := flag.String("port", "3000", "port to use")
	fileName := flag.String("fileName", "names.txt", "filename for list of names")
	flag.Parse()

	parsedPortInInt, err := strconv.ParseInt(*port, 10, 64)
	if err != nil {
		log.Fatalf("Error parsing port number, Port %s", *port)
	}
	if *numberOfNodes < 2 {
		log.Fatalf("Minimum 2 node is required for setting up Master / Slave, Nodes: %v", *numberOfNodes)
	}

	// sampleData := []string{"Sah", "Deepak", "Abhishek", "Sharma", "Zathura", "Harsh", "Jay", "Eight", "Nine"}
	sampleData := readTextFile(*fileName)
	divideWork(sampleData, *numberOfNodes-1)

	wg.Add(*numberOfNodes)
	for i := 0; i < *numberOfNodes; i++ {
		parsedPortInInt++
		node := createNode(*clusterIp, strconv.Itoa(int(parsedPortInInt)))
		go selectMasterNode(node)
	}
	wg.Wait()
	wg.Add(*numberOfNodes)
	for k, v := range nodes {
		if v == "master" {
			masterNode = k
			go listenOnPort(k)
		} else {
			go connectToNode(k)
		}
	}
	wg.Wait()
}

func getRequestObject(source NodeInfo, dest NodeInfo, dataToSort []string) data {
	return data{
		Source: NodeInfo{
			NodeId:     source.NodeId,
			NodeIpAddr: source.NodeIpAddr,
			Port:       source.Port,
			Status:     source.Status,
		},
		Dest: NodeInfo{
			NodeId:     dest.NodeId,
			NodeIpAddr: dest.NodeIpAddr,
			Port:       dest.Port,
			Status:     dest.Status,
		},
		Message: dataToSort,
	}
}

func createNode(ipAddr string, port string) NodeInfo {
	return NodeInfo{
		NodeId:     1,
		NodeIpAddr: ipAddr,
		Port:       port,
		Status:     "down",
	}
}

func connectToNode(node NodeInfo) {
	defer wg.Done()
	lAddr, _ := net.ResolveTCPAddr("tcp", node.NodeIpAddr+":"+node.Port)
	rAddr, _ := net.ResolveTCPAddr("tcp", masterNode.NodeIpAddr+":"+masterNode.Port)

	for {
		conn, err := net.DialTCP("tcp", lAddr, rAddr)
		if err == nil {
			node.Status = "up"
			request := getRequestObject(node, masterNode, []string{"Message"})
			_ = json.NewEncoder(conn).Encode(request)
			go handleResponseFromMaster(conn)
			break
		}
		fmt.Println("There is no Master node available. Waiting...", err)
		// break
	}
}

func listenOnPort(node NodeInfo) {
	defer wg.Done()
	ln, err := net.Listen("tcp", ":"+node.Port)
	if err != nil {
		log.Fatalf("Unable to create server at port: %s\n", node.Port)
	}
	node.Status = "up"

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("Unable to accept connection.")
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("Serving %s\n", conn.RemoteAddr().String())
	var request data
	_ = json.NewDecoder(conn).Decode(&request)
	fmt.Println("This is it: ", request)
	if request.Source.Status == "up" {
		// Divide the slice and give it to sort
		fmt.Println("Slave is Up")
		var response data
		response = getRequestObject(request.Dest, request.Source, request.Message)
		_ = json.NewEncoder(conn).Encode(&response)
	} else if request.Source.Status == "down" {
		// Save the result from the slave and close the connection
	}
}

// response from master node to slave node, set the status up and down
func handleResponseFromMaster(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	var response data
	_ = decoder.Decode(&response)
	fmt.Println("Response from master: ", response)
	// Sort the slice here, set the status to down and send it back to master
	var request data
	request = getRequestObject(response.Dest, response.Source, response.Message)
	request.Source.Status = "down"
	_ = json.NewEncoder(conn).Encode(&request)
}

// divide the sorting work to multiple nodes from the multi node cluster
// and merging the result came from different slave nodes, so master works
// as an aggregator to sort different slices
func divideWork(sampleData []string, numberOfSlaves int) {
	lenOfData := len(sampleData)
	chunkSize := (lenOfData + numberOfSlaves - 1) / numberOfSlaves

	for i := 0; i < lenOfData; i += chunkSize {
		end := i + chunkSize

		if end > lenOfData {
			end = lenOfData
		}
		sampleDataSlice := sampleData[i:end]
		sort.Strings(sampleDataSlice)
		dividedWork = append(dividedWork, sampleDataSlice)
	}
	fmt.Printf("Subarray to sort by node: %d: %#v\n", numberOfSlaves, dividedWork)
	// do iterative merging
	result := dividedWork[0]
	for j := 1; j < numberOfSlaves; j++ {
		result = mergeSortedSlice(result, dividedWork[j])
	}
	fmt.Printf("Final sorted Array: %#v\n", result)
}

// election algorithm to select master node, one node will be master at a moment
// other nodes will be slave, using mutex lock, the one will acquires lock will become master
// we can use consul for leader election and KV store
func selectMasterNode(node NodeInfo) {
	mutex.Lock()
	if masterKey {
		nodes[node] = "slave"
		wg.Done()
		mutex.Unlock()
		return
	}
	fmt.Println(node.Port)
	masterKey = true
	masterNode = node
	nodes[node] = "master"
	wg.Done()
	mutex.Unlock()
}

// read names from text file
// return array, slice
func readTextFile(fileName string) []string {
	fileBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return strings.Split(string(fileBytes), "\n")
}

// distributed merge sorting, two slices as input and return sorted list
func mergeSortedSlice(x []string, y []string) []string {
	result := make([]string, len(x)+len(y))

	i := 0
	for len(x) > 0 && len(y) > 0 {
		if x[0] < y[0] {
			result[i] = x[0]
			x = x[1:]
		} else {
			result[i] = y[0]
			y = y[1:]
		}
		i++
	}

	for j := 0; j < len(x); j++ {
		result[i] = x[j]
		i++
	}
	for j := 0; j < len(y); j++ {
		result[i] = y[j]
		i++
	}
	return result
}
