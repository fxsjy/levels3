// Copyright (c) 2020, Junyi Sun <ccnusjy@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package levels3

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"log"
	"os"
	"sync"
)

var errFileOpen = errors.New("leveldb/storage: file still open")

type OpenOption struct {
	Bucket   string
	Ak       string
	Sk       string
	Region   string
	Endpoint string
}

type S3StorageLock struct {
	ms *S3Storage
}

func (lock *S3StorageLock) Unlock() {
	ms := lock.ms
	if ms.slock == lock {
		ms.slock = nil
	}
	return
}

// S3Storage is a s3-backed storage.
type S3Storage struct {
	mu       sync.Mutex
	slock    *S3StorageLock
	meta     storage.FileDesc
	objStore *S3Client
	ramFiles map[string]*memFile
}

// NewS3Storage returns a new s3-backed storage implementation.
func NewS3Storage(opt OpenOption) (storage.Storage, error) {
	s3Client, err := GetS3Client(opt)
	if err != nil {
		return nil, err
	}
	return &S3Storage{
		objStore: s3Client,
		ramFiles: map[string]*memFile{},
	}, nil
}

func (ms *S3Storage) Lock() (storage.Locker, error) {
	log.Println("Lock")
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock != nil {
		return nil, storage.ErrLocked
	}
	ms.slock = &S3StorageLock{ms: ms}
	return ms.slock, nil
}

func (*S3Storage) Log(str string) {}

func (ms *S3Storage) SetMeta(fd storage.FileDesc) error {
	log.Println("SetMeta", fd)
	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	err := ms.objStore.PutBytes(fd.String(), ms.ramFiles[fd.String()].Bytes())
	if err != nil {
		return err
	}
	ms.meta = fd
	metaStr := fmt.Sprintf("%d %d", fd.Type, int64(fd.Num))
	err = ms.objStore.PutBytes("CURRENT", []byte(metaStr))
	if err != nil {
		return err
	}
	return nil
}

func (ms *S3Storage) GetMeta() (storage.FileDesc, error) {
	log.Println("GetMeta")
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.meta.Zero() {
		metaStr, err := ms.objStore.GetBytes("CURRENT")
		if err != nil {
			return storage.FileDesc{}, os.ErrNotExist
		}
		var fType int
		var fNum int64
		fmt.Sscanf(string(metaStr), "%d %d", &fType, &fNum)
		meta := storage.FileDesc{Type: storage.FileType(fType), Num: fNum}
		log.Println("GetMeta Remote", meta)
		ms.meta = meta
	}
	if ms.meta.Zero() {
		return storage.FileDesc{}, os.ErrNotExist
	}
	log.Println("GetMeta", ms.meta)
	return ms.meta, nil
}

func (ms *S3Storage) List(ft storage.FileType) ([]storage.FileDesc, error) {
	log.Println("List", ft)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	fdsAll, err := ms.objStore.List()
	if err != nil {
		return nil, err
	}
	var fds []storage.FileDesc
	existMap := map[string]bool{}
	for _, fd := range fdsAll {
		if fd.Type&ft != 0 {
			fds = append(fds, fd)
			existMap[fd.String()] = true
		}
	}
	for fname, _ := range ms.ramFiles {
		fd, _ := fsParseName(fname)
		if !existMap[fname] {
			fds = append(fds, fd)
		}
	}
	return fds, nil
}

func (ms *S3Storage) Open(fd storage.FileDesc) (storage.Reader, error) {
	log.Println("Open", fd)
	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if mfile, ok := ms.ramFiles[fd.String()]; ok {
		mfile.open = true
		return &memReader{Reader: bytes.NewReader(mfile.Bytes()), ms: ms, m: mfile, fd: fd}, nil
	}
	fname := fd.String()
	data, err := ms.objStore.GetBytes(fname)
	if err != nil {
		return nil, os.ErrNotExist
	}
	m := &memFile{Buffer: *bytes.NewBuffer(data), open: true}
	ms.ramFiles[fd.String()] = m
	return &memReader{Reader: bytes.NewReader(data), ms: ms, m: m, fd: fd}, nil
}

func (ms *S3Storage) Create(fd storage.FileDesc) (storage.Writer, error) {
	log.Println("Create", fd)
	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}
	m := &memFile{}
	m.open = true
	ms.mu.Lock()
	ms.ramFiles[fd.String()] = m
	ms.mu.Unlock()
	return &memWriter{memFile: m, ms: ms, fd: fd}, nil
}

func (ms *S3Storage) Remove(fd storage.FileDesc) error {
	log.Println("Remove", fd)
	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.ramFiles, fd.String())
	err := ms.objStore.Remove(fd.String())
	if err != nil {
		return os.ErrNotExist
	}
	return nil
}

func (ms *S3Storage) Rename(oldfd, newfd storage.FileDesc) error {
	log.Println("Rename", oldfd, newfd)
	if !storage.FileDescOk(oldfd) || !storage.FileDescOk(newfd) {
		return storage.ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}
	return nil
}

func (*S3Storage) Close() error {
	log.Println("storage Close")
	return nil
}

type memFile struct {
	bytes.Buffer
	open bool
}

type memReader struct {
	*bytes.Reader
	ms     *S3Storage
	m      *memFile
	fd     storage.FileDesc
	closed bool
}

func (mr *memReader) Close() error {
	log.Println("reader Close", mr.fd)
	mr.ms.mu.Lock()
	defer mr.ms.mu.Unlock()
	if mr.closed {
		return storage.ErrClosed
	}
	mr.m.open = false
	delete(mr.ms.ramFiles, mr.fd.String())
	return nil
}

type memWriter struct {
	*memFile
	ms     *S3Storage
	fd     storage.FileDesc
	closed bool
}

func (mw *memWriter) Sync() error {
	log.Println("writer Sync", mw.fd, "len", mw.memFile.Len())
	if mw.fd.Type == storage.TypeManifest {
		err := mw.ms.objStore.PutBytes(mw.fd.String(), mw.memFile.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func (mw *memWriter) Close() error {
	curLen := mw.memFile.Len()
	log.Println("writer Close", mw.fd, "len", curLen)
	fname := mw.fd.String()
	err := mw.ms.objStore.PutBytes(fname, mw.memFile.Bytes())
	if err != nil {
		return err
	}
	if mw.closed {
		return storage.ErrClosed
	}
	mw.memFile.open = false
	return nil
}
