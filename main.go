package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

type apiServerLb struct {
	Local  string
	RemoteServers []string
	HealthyServers []string
	rrCounter int
}

func (lb *apiServerLb) startHealthChecks() {
	for {
		newHealthyServers := make([]string, 0)
		for _, server := range lb.RemoteServers {
			// TODO add timeout
			resp, err := http.Get(fmt.Sprintf("https://%s/healthz", server))

			if err == nil && resp.StatusCode == 200 {
				newHealthyServers = append(newHealthyServers, server)
			}
		}

		lb.HealthyServers = newHealthyServers
		time.Sleep(30 * time.Second)
	}
}

func (lb *apiServerLb) chooseRemote() (string, error) {
	numberOfHealthyRemotes := len(lb.HealthyServers)
	if numberOfHealthyRemotes == 0 {
		return "", errors.New("No remote servers are Healthy")
	}
	picked := lb.rrCounter % numberOfHealthyRemotes
	return lb.HealthyServers[picked], nil
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
			log.Println("Error accepting connections in lb : %s", err)
			return err
		}

		remote, err := lb.chooseRemote()
		if err != nil {
			log.Println("Error trying to forward: %s\n", err)
			continue
		}

		remoteConn, err := net.Dial("tcp", remote)
		if err != nil {
			log.Println("Error trying to forward: %s\n", err)
			continue
		}

		go lb.forward(localConn, remoteConn)
	}
}

func (lb *apiServerLb) forward(localConn net.Conn, remoteConn net.Conn) {

	copyConn := func (writer, reader net.Conn) {
		_, err := io.Copy(writer, reader)
		if err != nil {
			log.Fatalf("io.Copy error: %s", err)
		}
		err = writer.Close()
		if err != nil {
			log.Fatalf("writer.Close error: %s", err)
		}
	}

	go copyConn(localConn, remoteConn)
	go copyConn(remoteConn, localConn)
}


func main() {
	for {
		lb := apiServerLb{
			HealthyServers: make([]string, 0),
			Local: "127.0.0.1:6443",
			RemoteServers: []string{"10.0.0.101:6443", "10.0.0.102:6443", "10.0.0.103:6443"},
			rrCounter: 1,
		}
		err := lb.Start()
		if err != nil {
			log.Println("Restarting lb because of HARD error: %s", err)
		}

		time.Sleep(1 * time.Second)
	}
}
