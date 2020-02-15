# levels3

Code Example:

```
opt := levels3.OpenOption{
  Bucket: "sjy3",
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
// Put, Get, NewIteraotr here, enjoy it:-)

```
