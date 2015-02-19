package logexp

import (
	"time"
	"sync"
	"sync/atomic"
	"log"
	"os"
	"strconv"
	"testing"
)

func init() {
	log.SetFlags(log.Llongfile | log.LstdFlags)
}

func BenchmarkSync15Writers(b *testing.B) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()

	s := NewSyncWAL(wal)

	work := newWriter(s)

	b.ResetTimer()

	var cnt int64
	var wg sync.WaitGroup
	for i:=0; i<15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prod := newProducer(anyFilter, work)
			start := time.Now()
			lastreport := start
			for total:=0; total<b.N;total++{
				next := atomic.AddInt64(&cnt, 1)
				if int(next) >= b.N {
					prod.Flush()
					log.Printf("wrote %02.f messages/sec,maxbatchsize=%d", float64(total) / float64(time.Now().Sub(start).Seconds()), prod.maxbatch)
					return
				}
				msg := "hello-" + strconv.Itoa(int(next))
				prod.WriteMsg([]byte(msg))
				elapsed := time.Now().Sub(lastreport)
				if  elapsed > time.Second {
					log.Printf("writing %02.f messages / sec", float64(total) / float64(elapsed.Seconds()))
					lastreport = time.Now()
				}
			}
		}()
	}
	wg.Wait()

	// unblock writer
	close(work.batch)
	<-work.ready
}

func BenchmarkWALWrite(b *testing.B) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := []byte("hello-" + strconv.Itoa(i+1))
		n, err := wal.Write(data)
		if err != nil {
			panic(err)
		}
		if n != len(data) {
			b.Fatal("bytes written does not match bytes given")
		}
	}
}

func BenchmarkWALRead(b *testing.B) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()

	for i := 0; i < b.N; i++ {
		data := []byte("hello-" + strconv.Itoa(i+1))
		n, err := wal.Write(data)
		if err != nil {
			panic(err)
		}
		if n != len(data) {
			b.Fatal("bytes written does not match bytes given")
		}
	}
	b.ResetTimer()
	inputlen := 1 << 10
	buf := make([]byte, inputlen)
	var (
		msgs                    [][]byte
		lastcounter, newcounter int
		err                     error
	)
	for i := 0; i < b.N; {
		buf, msgs, newcounter, err = wal.Fetch(buf, lastcounter)
		if err != nil {
			panic(err)
		}
		i += len(msgs)
		for i, msg := range msgs {
			exp := "hello-" + strconv.Itoa(newcounter+i)
			if string(msg) != exp {
				b.Fatalf("msg %d: expected %s but got %s", newcounter+i, exp, string(msg))
			}
		}
		lastcounter = newcounter + len(msgs) - 1
	}
}

func TestNewWAL(t *testing.T) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()
}

func TestWALWrite(t *testing.T) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()

	n, err := wal.Write([]byte("hello"))
	if err != nil {
		panic(err)
	}
	if n != len("hello") {
		t.Fatal("bytes written does not match bytes given")
	}
}

func TestWALWriteBatch(t *testing.T) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()

	batch := [][]byte{ []byte("hello"), []byte("there") }
	n, nmsg, err := wal.WriteBatch(batch)
	if err != nil {
		panic(err)
	}
	if n != len("hello") + len("there") {
		t.Fatal("bytes written does not match bytes given")
	}
	if nmsg != len(batch) {
		t.Fatal("messages written does not match batch size")
	}
}

func TestWALWriteRead(t *testing.T) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()

	n, err := wal.Write([]byte("hello"))
	if err != nil {
		panic(err)
	}
	if n != len("hello") {
		t.Fatal("bytes written does not match bytes given")
	}
	inputlen := 1 << 10
	buf, msgs, counter, err := wal.Fetch(make([]byte, inputlen), 0)
	if err != nil {
		panic(err)
	}
	if len(buf) != inputlen {
		t.Fatal("expected input buf to be returned with same size")
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 msg, but got %d", len(msgs))
	}
	if counter != 1 {
		t.Fatalf("expected counter 0, but got %d", counter)
	}
	if string(msgs[0]) != "hello" {
		t.Fatalf("expected 'hello' but but '%s'", string(msgs[0]))
	}
}

func TestWALWriteReadMany(t *testing.T) {
	if exists("testdata") {
		panic("testdata directory already exists, clean clean it out")
	}
	if err := os.Mkdir("testdata", 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll("testdata")

	wal := newTestWAL()
	if err := wal.Open(); err != nil {
		panic(err)
	}
	defer wal.Close()

	for i := 0; i < 1e3; i++ {
		data := []byte("hello-" + strconv.Itoa(i+1))
		n, err := wal.Write(data)
		if err != nil {
			panic(err)
		}
		if n != len(data) {
			t.Fatal("bytes written does not match bytes given")
		}
	}

	inputlen := 1 << 10
	buf := make([]byte, inputlen)
	var (
		msgs                    [][]byte
		lastcounter, newcounter int
		err                     error
	)
	for {
		buf, msgs, newcounter, err = wal.Fetch(buf, lastcounter)
		if err != nil {
			panic(err)
		}
		if len(msgs) == 0 {
			break
		}
		if newcounter != lastcounter+1 {
			t.Fatalf("expected counter to increment without skipping")
		}
		for i, msg := range msgs {
			exp := "hello-" + strconv.Itoa(newcounter+i)
			if string(msg) != exp {
				t.Fatalf("msg %d: expected %s but got %s", newcounter+i, exp, string(msg))
			}
		}
		lastcounter = newcounter + len(msgs) - 1
	}
}
