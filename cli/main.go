package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xuperchain/xuperchain/core/kv/s3"
	"time"
)

func main() {
	opt := levels3.OpenOption{
		Bucket: "sjy5",
		Path:   "mydb1/sub2",
		Ak:     "",
		Sk:     "",
		Region: "ap-northeast-1",
	}
	st, err := levels3.NewS3Storage(opt)
	if err != nil {
		panic(err)
		fmt.Println(st)
	}
	db, err := leveldb.Open(st, nil)
	defer db.Close()
	if err != nil {
		panic(err)
	}
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
	}
	for j := 1; j <= 1; j++ {
		for i := 1; i <= 10000; i++ {
			key := fmt.Sprintf("key_%d_%d", j, i)
			err = db.Put([]byte(key), []byte("WORLD"), nil)
			if err != nil {
				panic(err)
			}
		}
		time.Sleep(2 * time.Second)
		fmt.Println(j)
	}
}
