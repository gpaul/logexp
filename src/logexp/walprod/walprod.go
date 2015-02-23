package main

import (
	"encoding/json"
	"flag"
	"log"
	"logexp"
	"net"
	"strconv"
	"time"
)

func main() {
	dialpath := flag.String("dialpath", "@/wal", "the unix socket to dial to")
	flag.Parse()

	conn, err := net.Dial("unix", *dialpath)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)

	last := 0
	start := time.Now()
	for i := 0; ; i++ {
		msg := logexp.Message{"hello " + strconv.Itoa(i)}
		if err := enc.Encode(msg); err != nil {
			panic(err)
		}
		elapsed := time.Now().Sub(start)
		if elapsed > time.Second {
			log.Printf("%d msgs/s", int(float64(i-last)/elapsed.Seconds()))
			start = time.Now()
			last = i
		}
		if i%1000 == 0 {
			time.Sleep(1 * time.Second)
		}
	}
}
