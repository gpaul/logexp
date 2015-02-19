package main

import (
	"time"
	"log"
	"logexp"
	"strconv"
	"encoding/json"
	"flag"
	"net"
)
	
func main() {
	dialpath := flag.String("dialpath", "@/wal", "the unix socket to dial to")

	conn, err := net.Dial("unix", *dialpath)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)

	last := 0
	start := time.Now()
	for i:=0; ; i++ {
		msg := logexp.Message{"hello " + strconv.Itoa(i)}
		if err := enc.Encode(msg); err != nil {
			panic(err)
		}
		elapsed := time.Now().Sub(start)
		if elapsed > time.Second {
			log.Printf("%d msgs/s", int(float64(i-last) / elapsed.Seconds()))
			start = time.Now()
			last = i
		}
	}
}
