package main

import (
	"encoding/json"
	"sync"
	"log"
	"flag"
	"net"
	"logexp"
)

func init() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
}

func main() {
	dir := flag.String("dir", "wal", "the directory where the WAL will be created")
	buffersize := flag.Int("buffersize", 100<<20, "the size in bytes of the individual WAL buffers")
	numbuffers := flag.Int("numbuffers", 10, "the number of buffers comprising the WAL circular buffer pool")
	listenpath := flag.String("unix-socket", "@/wal", "the path of the unix socket to listen on")

	flag.Parse()

	w := logexp.NewWAL()
	w.Dir(*dir)
	w.BufferSize(*buffersize)
	w.NumBuffers(*numbuffers)

	listener, err := net.Listen("unix", *listenpath)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	if err := w.Open(); err != nil {
		panic(err)
	}

	log.Printf("opened at %s", *dir)

	s := logexp.NewSyncWAL(w)
	work := logexp.NewWriter(s)

	var wg sync.WaitGroup

	wg.Add(1)
	go listen(listener, work, &wg)
	log.Printf("accepting connections at %s", *listenpath)
	wg.Wait()
	log.Printf("shutting down")
}

func listen(listener net.Listener, work logexp.Work, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			return
		}

		wg.Add(1)
		go read(conn, work, wg)
	}
}

func read(conn net.Conn, work logexp.Work, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()
	prod := logexp.NewProducer(logexp.AnyFilter, work)
	dec := json.NewDecoder(conn)
	var msg logexp.Message
	for {
		err := dec.Decode(&msg)
		if err != nil {
			log.Printf("decode error: %v", err)
			return
		}
		buf, err := json.Marshal(msg)
		if err != nil {
			log.Printf("marshal error: %v", err)
			return
		}
		prod.WriteMsg(buf)
	}
}
