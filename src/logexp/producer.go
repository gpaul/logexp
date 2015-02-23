package logexp

import (
	"log"
)

type Producer struct {
	filter ByteFilter
	work Work
	batch [][]byte
	maxbatch int
}

func NewProducer(filter ByteFilter, work Work) *Producer {
	return &Producer{filter, work, nil, 0}
}

func (p *Producer) WriteMsg(msg []byte) {
	if ok, err := p.filter(msg); !ok || err != nil {
		if err != nil {
			log.Printf("could not filter msg: %v", err)
		}
		return
	}
	p.batch = append(p.batch, msg)
	select {
	case <-p.work.Ready:
		p.work.Batch<-p.batch
		if len(p.batch) > p.maxbatch {
			p.maxbatch = len(p.batch)
		}
		p.batch = nil
	default:
	}
}

func (p *Producer) Flush() {
	select {
	case <-p.work.Ready:
		p.work.Batch<-p.batch
		if len(p.batch) > p.maxbatch {
			p.maxbatch = len(p.batch)
		}
		p.batch = nil
	default:
	}
}
