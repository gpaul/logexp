package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"logexp"
	"net"
	"path/filepath"
	"sync"
	"time"
)

func init() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
}

func main() {
	dir := flag.String("dir", "wal", "the directory where the WAL will be created")
	buffersize := flag.Int("buffersize", 100<<20, "the size in bytes of the individual WAL buffers")
	numbuffers := flag.Int("numbuffers", 10, "the number of buffers comprising the WAL circular buffer pool")
	listenpath := flag.String("unix-socket", "@/wal", "the path of the unix socket to listen on")
	configpath := flag.String("config", "./config.json", "the path to the config.json file")
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

	dialConsumers(*configpath, s, &wg)

	wg.Add(1)
	go listen(listener, work, &wg)
	log.Printf("accepting connections at %s", *listenpath)

	wg.Wait()
	log.Printf("shutting down")
}

func dialConsumers(configpath string, s *logexp.SyncWAL, wg *sync.WaitGroup) {
	config, err := loadConfig(configpath)
	if err != nil {
		log.Printf("cannot load configfile: %s", configpath)
		return
	}
	for _, consumer := range config.Consumers {
		conn, err := net.Dial("unix", consumer.Raddr)
		if err != nil {
			log.Printf("failed to dial consumer at %s", consumer.Raddr)
			return
		}
		wg.Add(1)
		go deliver_messages(s, conn, consumer.Filters, wg)
	}
}

func deliver_messages(s *logexp.SyncWAL, conn net.Conn, filters []string, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()
	enc := json.NewEncoder(conn)
	var err error
	buf := make([]byte, 1<<10)
	msgs := [][]byte{}
	lastcounter := 0
	newcounter := 0
	var i int
	for {
		buf, msgs, newcounter, err = s.Fetch(buf, lastcounter)
		if err != nil {
			panic(err)
		}
		log.Printf("lastcounter=%d,len(msgs)=%d,newcounter=%d", lastcounter, len(msgs), newcounter)
		for _, msg := range msgs {
			// TODO: add message filtering here
			if err := enc.Encode(logexp.Message{string(msg)}); err != nil {
				log.Printf("encode error: %v", err)
				return
			}
			i++
			if i%1000 == 0 {
				log.Printf("lastcounter=%d,len(msgs)=%d,newcounter=%d", lastcounter, len(msgs), newcounter)
				log.Printf("sent %d messages", i)
			}
		}
		if newcounter != lastcounter {
			lastcounter = newcounter + len(msgs) - 1
		}
		if len(msgs) == 0 {
			// if we've caught up, don't sit and spin but rather
			// sleep a bit and see if there are new messages afterwards
			time.Sleep(1 * time.Second)
		}
	}
}

func loadConfig(configpath string) (*Config, error) {
	config := new(Config)
	path, err := filepath.Abs(configpath)
	if err != nil {
		return nil, err
	}
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(buf, config)
	if err != nil {
		return nil, err
	}
	return config, nil
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

type Config struct {
	Consumers []Consumer
}

type Consumer struct {
	Raddr string
	// Add filter support
	Filters []string
}
