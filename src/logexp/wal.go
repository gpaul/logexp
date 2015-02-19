package logexp

import (
	"errors"
	"sync"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

const PROT_RDWR = syscall.PROT_READ | syscall.PROT_WRITE
const offsetEntrySize = 4 + 4 // 32-bit counter, 32-bit data offset

// SyncWAL synchronizes Read/Write operations on a WAL.
type SyncWAL struct {
	lk sync.RWMutex
	w *WAL
}

func NewSyncWAL(w *WAL) *SyncWAL {
	return &SyncWAL{w: w}
}

func (s *SyncWAL) Write(buf []byte) (int, error) {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.w.write(buf)
}

func (s *SyncWAL) WriteBatch(bufs [][]byte) (bytesWritten int, msgsWritten int, err error) {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.w.writeBatch(bufs)
}

func (s *SyncWAL) Fetch(buf []byte, last int) ([]byte, [][]byte, int, error) {
	s.lk.RLock()
	defer s.lk.RUnlock()
	return s.w.fetch(buf, last)
}

type WAL struct {
	buffersize int
	numbuffers int
	dir        string
	buffers    pool
	counter    int64
}

func mustAbs(path string) string {
	dir, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	return dir
}

func NewWAL() *WAL {
	// override these defaults by calling the appropriate setters
	return &WAL{
		100 << 20,
		10,
		mustAbs("wal"),
		nil,
		0,
	}
}

func (w *WAL) BufferSize(n int) {
	w.buffersize = n
}

func (w *WAL) NumBuffers(n int) {
	w.numbuffers = n

}

func (w *WAL) Dir(dir string) {
	w.dir = mustAbs(dir)
}

func (w *WAL) Open() error {
	if !exists(w.dir) {
		err := os.MkdirAll(w.dir, 0755)
		if err != nil {
			return err
		}
	}
	entries, err := w.files("*")
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		panic("not implemented")
	}
	var buffers pool
	for i := 0; i < w.numbuffers; i++ {
		buffername := "buffer-" + strconv.Itoa(i+1)
		buffer, err := newBuffer(w.filepath(buffername), w.buffersize, &w.counter)
		if err != nil {
			if err := buffers.Close(); err != nil {
				panic(err)
			}
			if err := buffers.RemoveAll(); err != nil {
				panic(err)
			}
			return err
		}
		buffers = append(buffers, buffer)
	}
	w.buffers = buffers
	return nil
}

func (w *WAL) write(buf []byte) (int, error) {
	if len(buf) > w.buffersize {
		return 0, syscall.ENOSPC
	}
	n, err := w.buffers[0].Write(buf)
	if err == nil {
		return n, nil
	}
	if err == syscall.ENOSPC {
		// front buffer is full. time to rotate
		w.buffers.rotate()
		return w.buffers[0].Write(buf)
	}
	return 0, err
}

func (w *WAL) writeBatch(bufs [][]byte) (bytesWritten int, msgsWritten int, err error) {
	for i, buf := range bufs {
		n, err := w.write(buf)
		if err != nil {
			return bytesWritten, i, err
		}
		bytesWritten += n
	}
	return bytesWritten, len(bufs), nil
}

func (w *WAL) fetch(buf []byte, last int) ([]byte, [][]byte, int, error) {
	// find an appropriate buffer
	var b *buffer

	// TODO(gpaul): when the WAL becomes lock-free we'll have
	// to avoid the back of the buffer pool
	for i := len(w.buffers) - 1; i >= 0; i-- {
		if w.buffers[i].hasMoreAfter(last) {
			b = w.buffers[i]
			break
		}
	}
	if b == nil {
		// none of the buffers contained entries
		// newer than the last counter
		// so we just return
		return buf, nil, last, nil
	}
	return b.Fetch(buf, last)
}

// Close closes all the underlying buffers
// If err is not nil the caller should panic
func (w *WAL) Close() error {
	for _, b := range w.buffers {
		if err := b.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all the underlying buffers
// If err is not nil the caller should panic
func (w *WAL) RemoveAll() error {
	for _, b := range w.buffers {
		if err := b.RemoveAll(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) files(parts ...string) ([]string, error) {
	return filepath.Glob(w.filepath(parts...))
}

func (w *WAL) filepath(parts ...string) string {
	parts = append([]string{w.dir}, parts...)
	return filepath.Join(parts...)
}

type buffer struct {
	buf, offsetbuf            []byte
	datafile, offsetfile      *os.File
	path                      string
	size                      int
	nbuf, noff                int
	counter                   *int64
	firstCounter, lastCounter int
}

func newBuffer(path string, size int, counter *int64) (*buffer, error) {
	b := &buffer{nil, nil, nil, nil, path, size, 0, 0, counter, -1, -1}
	if exists(b.datafilePath()) {
		return nil, errors.New("datafile already exists")
	}
	if exists(b.offsetfilePath()) {
		return nil, errors.New("datafile already exists")
	}
	var err error
	if err := b.openDatafile(); err != nil {
		return nil, err
	}
	if err := b.openOffsetfile(); err != nil {
		check(b.Close)
		check(b.RemoveAll)
		return nil, err
	}

	buf, err := syscall.Mmap(int(b.datafile.Fd()), 0, b.size, PROT_RDWR, syscall.MAP_PRIVATE)
	if err != nil {
		check(b.Close)
		check(b.RemoveAll)
		return nil, os.NewSyscallError("mmap", err)
	}
	b.buf = buf

	offsetbuf, err := syscall.Mmap(int(b.offsetfile.Fd()), 0, b.offsetfileSize(), PROT_RDWR, syscall.MAP_PRIVATE)
	if err != nil {
		check(b.Close)
		check(b.RemoveAll)
		return nil, os.NewSyscallError("mmap", err)
	}
	b.offsetbuf = offsetbuf

	return b, nil
}

func (b *buffer) datafilePath() string {
	return b.path + ".dat"
}

func (b *buffer) openDatafile() error {
	file, err := os.Create(b.datafilePath())
	if err != nil {
		return err
	}
	err = syscall.Fallocate(int(file.Fd()), 0, 0, int64(b.size))
	if err != nil {
		if err := os.Remove(b.datafilePath()); err != nil {
			panic(err)
		}
		return os.NewSyscallError("fallocate", err)
	}
	b.datafile = file
	return nil
}

func (b *buffer) offsetfilePath() string {
	return b.path + ".idx"
}

func (b *buffer) offsetfileSize() int {
	// this 128 is pretty arbitrary - it is difficult to tune
	// this offsetfile size for different workloads so we're going
	// with the segment to index size ratio that kafka has
	// http://kafka.apache.org/documentation.html
	// see (segment.bytes) and (segment.index.bytes)
	size := b.size / 128
	if size < 128 {
		return 128
	}
	return size
}

func (b *buffer) openOffsetfile() error {
	file, err := os.Create(b.offsetfilePath())
	if err != nil {
		return err
	}
	err = syscall.Fallocate(int(file.Fd()), 0, 0, int64(b.offsetfileSize()))
	if err != nil {
		if err := os.Remove(b.offsetfilePath()); err != nil {
			panic(err)
		}
		return os.NewSyscallError("fallocate", err)
	}
	b.offsetfile = file
	return nil
}

func (b *buffer) Write(buf []byte) (int, error) {
	if b.nbuf+len(buf)+4 >= len(b.buf) {
		return 0, syscall.ENOSPC
	}
	if b.noff+offsetEntrySize >= len(b.offsetbuf) {
		return 0, syscall.ENOSPC
	}
	(*b.counter)++
	counter := *b.counter
	if b.firstCounter == -1 {
		// we write the starting counter at the very start of the buffer
		putLittleEndianInt64(b.offsetbuf[b.noff:], counter)
		b.noff += 8
		b.firstCounter = int(counter)
	}
	offset := b.nbuf
	// write the msg length in the buffer
	putLittleEndianUint32(b.buf[offset:], uint32(len(buf)))
	n := copy(b.buf[offset+4:], buf)
	b.nbuf += n + 4
	// write the counter in the offset buffer
	relativeCounter := uint32(counter - int64(b.firstCounter))
	putLittleEndianUint32(b.offsetbuf[b.noff:], relativeCounter)
	b.noff += 4
	// write the offset in the offset buffer
	putLittleEndianUint32(b.offsetbuf[b.noff:], uint32(offset))
	b.noff += 4
	b.lastCounter = int(counter)
	return n, nil
}

func (b *buffer) Fetch(buf []byte, from int) ([]byte, [][]byte, int, error) {
	counter, offset := b.nextOffset(from)
	if offset < 0 {
		return buf, nil, from, nil
	}
	wbuf := buf
	msgs := make([][]byte, 0, 10)

	lim := offset + 4 + len(buf)
	if lim > len(b.buf) {
		lim = len(b.buf)
	}
	// we mlock to cause the scheduler to schedule another
	// goroutine while we block on IO here
	if err := syscall.Mlock(b.buf[offset:lim]); err != nil {
		return nil, nil, 0, os.NewSyscallError("mlock", err)
	}

	for {
		msglen := decodeMsgLen(b.buf[offset:])
		if msglen == 0 {
			// we've reached the end of the buffer
			break
		}
		offset += 4
		if msglen > len(wbuf) {
			if len(msgs) == 0 {
				// this message is too big for the given buffer
				// and we haven't written a single msg.
				// grow the input buffer
				if err := syscall.Mlock(b.buf[offset : offset+msglen]); err != nil {
					return nil, nil, 0, os.NewSyscallError("mlock", err)
				}
				buf = make([]byte, msglen)
				copy(buf, b.buf[offset:])
				msgs = append(msgs, buf)
				return buf, msgs, counter, nil
			}
			// we've already decoded some msgs so no reason to
			// allocate a bigger buffer.
			// since the next msg doesn't fit, just return what we've got so far
			return buf, msgs, counter, nil
		}
		// this msg will fit in the provided buffer, add it!
		copy(wbuf, b.buf[offset:offset+msglen])
		msgs = append(msgs, wbuf[:msglen])
		offset += msglen
		wbuf = wbuf[msglen:]
	}
	return buf, msgs, counter, nil
}

func (b *buffer) hasMoreAfter(counter int) bool {
	return counter < b.lastCounter
}

func (b *buffer) nextOffset(last int) (int, int) {
	if last < b.firstCounter {
		return b.firstCounter, 0
	}
	if last >= b.lastCounter {
		return -1, -1
	}
	buf := b.offsetbuf[8:]
	next := last + 1
	relcnt := next - b.firstCounter
	buf = buf[relcnt*8:]
	counter, offset := decodeCounterAndOffset(buf)
	return counter + b.firstCounter, offset
}

func putLittleEndianUint32(buf []byte, n uint32) {
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
	buf[2] = byte(n >> 16)
	buf[3] = byte(n >> 24)
}

func putLittleEndianInt64(buf []byte, sn int64) {
	n := uint64(sn)
	buf[0] = byte(n)
	buf[1] = byte(n >> 8)
	buf[2] = byte(n >> 16)
	buf[3] = byte(n >> 24)
	buf[4] = byte(n >> 32)
	buf[5] = byte(n >> 40)
	buf[6] = byte(n >> 48)
	buf[7] = byte(n >> 56)
}

func getLittleEndianUint32(buf []byte) uint32 {
	return uint32(buf[3])<<24 | uint32(buf[2])<<16 | uint32(buf[1])<<8 | uint32(buf[0])
}

func decodeMsgLen(buf []byte) int {
	return int(getLittleEndianUint32(buf))
}

func decodeCounterAndOffset(buf []byte) (int, int) {
	return int(getLittleEndianUint32(buf)), int(getLittleEndianUint32(buf[4:]))
}

func (b *buffer) zero() error {
	if err := syscall.Ftruncate(int(b.datafile.Fd()), 0); err != nil {
		return os.NewSyscallError("ftruncate", err)
	}
	if err := syscall.Fallocate(int(b.datafile.Fd()), 0, 0, int64(b.size)); err != nil {
		return os.NewSyscallError("fallocate", err)
	}
	if err := syscall.Ftruncate(int(b.offsetfile.Fd()), 0); err != nil {
		return os.NewSyscallError("ftruncate", err)
	}
	if err := syscall.Fallocate(int(b.offsetfile.Fd()), 0, 0, int64(b.offsetfileSize())); err != nil {
		return os.NewSyscallError("fallocate", err)
	}
	b.nbuf = 0
	b.noff = 0
	b.firstCounter = -1
	b.lastCounter = -1
	return nil
}

// Close closes the underlying files.
// If err is not nil, the caller should panic.
func (b *buffer) Close() error {
	if b.buf != nil {
		if err := syscall.Munmap(b.buf); err != nil {
			return err
		}
	}
	if b.offsetbuf != nil {
		if err := syscall.Munmap(b.buf); err != nil {
			return err
		}
	}
	if b.datafile != nil {
		if err := b.datafile.Close(); err != nil {
			return err
		}
	}
	if b.offsetfile != nil {
		if err := b.offsetfile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// RemoveAll deletes the datafile and offsetfile
// If err is not nil the files are still lying around.
// The caller should probably panic and a severe log should be raised.
func (b *buffer) RemoveAll() error {
	if err := os.Remove(b.datafilePath()); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.Remove(b.offsetfilePath()); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

type pool []*buffer

func (p pool) Close() error {
	for _, buffer := range p {
		if err := buffer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p pool) RemoveAll() error {
	for _, buffer := range p {
		if err := buffer.RemoveAll(); err != nil {
			return err
		}
	}
	return nil
}

func (p pool) rotate() {
	last := p[len(p)-1]
	check(last.zero)
	copy(p[1:], p)
	p[0] = last
}

func newTestWAL() *WAL {
	wal := NewWAL()
	wal.BufferSize(1 << 20)
	wal.NumBuffers(10)
	wal.Dir("testdata/wal")
	return wal
}

func check(f func() error) {
	if err := f(); err != nil {
		panic(err)
	}
}

func exists(filename string) bool {
	_, err := os.Lstat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	panic(err)
}
