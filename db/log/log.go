package log

import (
	"os"
	"path"

	"github.com/go-distributed/xtree/db/message"
)

const (
	logFilename = "records.log"
)

type Log struct {
	writeFile, readFile *os.File
	encoder             *encoder
}

func Create(dataDir string) (l *Log, err error) {
	p := path.Join(dataDir, logFilename)

	var writeFile, readFile *os.File
	if writeFile, err = os.OpenFile(p,
		os.O_WRONLY|os.O_APPEND|os.O_CREATE,
		0600); err != nil {
		return
	}
	if readFile, err = os.Open(p); err != nil {
		return
	}
	l = &Log{
		writeFile: writeFile,
		readFile:  readFile,
		encoder:   newEncoder(writeFile),
	}
	return
}

func Exist(dataDir string) bool {
	p := path.Join(dataDir, logFilename)
	_, err := os.Stat(p)
	return err == nil
}

func (l *Log) GetRecord(offset int64) (r *message.Record, err error) {
	if _, err = l.readFile.Seek(offset, 0); err != nil {
		return
	}
	decoder := newDecoder(l.readFile)
	r = &message.Record{}
	err = decoder.decode(r)
	return
}

func (l *Log) Append(r *message.Record) (offset int64, err error) {
	if offset, err = l.writeFile.Seek(0, os.SEEK_CUR); err != nil {
		return
	}
	if err = l.encoder.encode(r); err != nil {
		return
	}
	err = l.encoder.flush()
	return offset, err
}
