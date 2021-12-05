package bilinetdrive

import (
	"container/list"
	"errors"
	"net/http"
	"sync"
	"time"
)

const (
	nodeImageWidth     = 512
	nodeImageMaxHeight = 9000
	fileImageWidth     = 512
	fileImageMaxHeight = 512
)

var (
	uploadThreads        = 16
	downloadThreads      = 16
	fileNowUsingList     = list.New()
	fileNowUsingListLock sync.Mutex
	retryTimes           = 5
	retryWaitTime        = time.Millisecond * 100
	SESSDATA             = ""
	rootNodeHash         = ""
	UserAgent            = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0"
	httpClient           = http.Client{Timeout: time.Second * 5}
)

var (
	nodeRWLock              sync.Mutex
	nodeCache               map[string]*CacheNodeStruct
	nodeCacheManagerStarted = false
	nodeCacheStayTime       = 300 //s
	nodeCacheScanTime       = time.Second * 1
	nodeUploadJobList       = list.New()
	nodeUploadJobListLock   sync.Mutex
	nodeUploadThreads       = 16
)

type JobQueueStruct struct {
	lock *sync.Mutex
	list *list.List
}

type SafeUploadNodeStruct struct {
	lock   *sync.Mutex
	mapObj map[string][]string
}

type CacheNodeStruct struct {
	nodeData   map[string][]string
	createTime int64
}

func NotSetARootNodeYet() error {
	return errors.New("not set a root node yet")
}

func PathDoesNotExist() error {
	return errors.New("path does not exists")
}

func FolderDoesNotExist() error {
	return errors.New("folder does not exists")
}

func FileDoesNotExist() error {
	return errors.New("file does not exists")
}

func NotAFile() error {
	return errors.New("not A File")
}

func NodeDoesNotExist() error {
	return errors.New("node does not exists")
}

func NameExisted() error {
	return errors.New("name Existed")
}

func FileIsUsing() error {
	return errors.New("file is using")
}

func SetSESSDATA(sessdata string) { //要修改或上传必须要有SESSDATA
	SESSDATA = sessdata
}

func CostumeUserAgent(useragent string) { //自定义UA
	UserAgent = useragent
}
