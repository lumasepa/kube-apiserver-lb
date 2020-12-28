package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type HealthCheck struct {
	Period int `yaml:"check_period"`
	UpThreshold int `yaml:"up_threshold"`
	DownThreshold int `yaml:"down_threshold"`
}

type Configuration struct {
	KubeApiServers []string `yaml:"kube_apiservers"`
	ListenAddr string `yaml:"listen_addr"`
	HealthCheck HealthCheck `yaml:"health_check"`
}

func readConfiguration(path string) (*Configuration, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	config := &Configuration{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

type apiServerLb struct {
	Local  string
	RemoteServers []string
	HealthyServers []string
	HealthyServersLock sync.RWMutex
	rrCounter int
	healthCheckRules HealthCheck
	httpClient *http.Client
}

func (lb *apiServerLb) startHealthChecks() {
	for {
		newHealthyServers := make([]string, 0)
		for _, server := range lb.RemoteServers {
			resp, err := lb.httpClient.Get(fmt.Sprintf("https://%s/healthz", server))

			if err == nil && resp.StatusCode == 200 {
				newHealthyServers = append(newHealthyServers, server)
			} else {
				var errStr string

				if err != nil {
					errStr = err.Error()
				} else {
					errStr = fmt.Sprintf("HTTP status code : %d", resp.StatusCode)
				}
				log.Printf("kube-apiserver %s is not healthy : %s", server, errStr)
			}
		}

		lb.HealthyServersLock.Lock()
		lb.HealthyServers = newHealthyServers
		lb.HealthyServersLock.Unlock()

		time.Sleep(time.Duration(lb.healthCheckRules.Period) * time.Second)
	}
}

func (lb *apiServerLb) chooseRemote() (string, error) {
	lb.HealthyServersLock.RLock()

	numberOfHealthyRemotes := len(lb.HealthyServers)
	if numberOfHealthyRemotes == 0 {
		return "", errors.New("no remote servers are Healthy")
	}
	pickedIdx := lb.rrCounter % numberOfHealthyRemotes
	picked :=  lb.HealthyServers[pickedIdx]

	lb.HealthyServersLock.RUnlock()

	return picked, nil
}

func (lb *apiServerLb) removeHealthyRemote(remote string) {
	newHealthyServers := make([]string, 0)

	lb.HealthyServersLock.Lock()
	for _, server := range lb.HealthyServers {
		if server != remote {
			newHealthyServers = append(newHealthyServers, server)
		}
	}

	lb.HealthyServers = newHealthyServers
	lb.HealthyServersLock.Unlock()
}

func (lb *apiServerLb) Start() error {
	go lb.startHealthChecks()

	listener, err := net.Listen("tcp", lb.Local)
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		localConn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connections in lb : %s", err)
			return err
		}

		remote, err := lb.chooseRemote()
		if err != nil {
			log.Printf("Error trying to forward: %s\n", err)
			continue
		}

		remoteConn, err := net.Dial("tcp", remote)
		if err != nil {
			log.Printf("Error trying to forward: %s\n", err)
			lb.removeHealthyRemote(remote)
			continue
		}

		go lb.forward(localConn, remoteConn)
	}
}

func CloseAndLog(conn net.Conn) {
	err := conn.Close()
	if err != nil {
		log.Printf("Error closing socket: %s", err)
	}
}

func (lb *apiServerLb) forward(localConn net.Conn, remoteConn net.Conn) {

	copyConn := func (writer, reader net.Conn) {
		defer CloseAndLog(writer)
		defer CloseAndLog(reader)
		_, err := io.Copy(writer, reader)
		if err != nil {
			log.Printf("io.Copy error: %s", err)
		}
	}

	go copyConn(localConn, remoteConn)
	go copyConn(remoteConn, localConn)
}


func main() {
	path := flag.String("config", "./config.yaml", "config file")
	flag.Parse()

	config, err := readConfiguration(*path)
	if err != nil {
		log.Fatalf("error reading configuration : %s", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 5 * time.Second,
	}

	for {
		lb := apiServerLb{
			HealthyServers: make([]string, 0),
			Local: config.ListenAddr,
			RemoteServers: config.KubeApiServers,
			rrCounter: 1,
			healthCheckRules: config.HealthCheck,
			httpClient: client,
		}
		err := lb.Start()
		if err != nil {
			log.Printf("Restarting lb because of HARD error: %s", err)
		}

		time.Sleep(1 * time.Second)
	}
}
