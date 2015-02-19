package logexp

type Work struct {
	Ready chan struct{}
	Batch chan [][]byte
}

func NewWriter(s *SyncWAL) (Work) {
	readyCh := make(chan struct{})

	// we make batchCh buffered so we NEVER
	// block a writer for even the briefest moment
	batchCh := make(chan [][]byte, 1)

	go func() {
		readyCh <- struct{}{}
		for batch := range batchCh {
			_, _, err := s.WriteBatch(batch)
			if err != nil {
				panic(err)
			}
			readyCh <- struct{}{}
		}
		close(readyCh)
	}()

	return Work{readyCh, batchCh}
}
