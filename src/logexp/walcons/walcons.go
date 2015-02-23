package main

import (
	"encoding/json"
	"sync"
	"logexp"
	"flag"
	"net"
	"log"
)

func init() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
}

func main() {
	listenpath := flag.String("listenpath", "@/walcons", "the unix socket to listen on")
	flag.Parse()

	listener, err := net.Listen("unix", *listenpath)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go listen(listener, &wg)
	wg.Wait()
	log.Printf("shutting down")
}

func listen(listener net.Listener, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			return
		}
		wg.Add(1)
		go read(conn, wg)
	}
}

func read(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()

	var i int
	dec := json.NewDecoder(conn)
	for {
		if i %1000 == 0 {
			log.Printf("received %d messages", i)
		}
		var msg logexp.Message
		err := dec.Decode(&msg)
		if err != nil {
			log.Printf("decode error: %v", err)
			return
		}
		i++
	}
}
