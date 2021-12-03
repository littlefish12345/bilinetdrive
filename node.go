package bilinetdrive

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/ioutil"
	"runtime"
	"sync"
	"time"
)

//除非有信心不然不要用这里的函数！

func DecodeNode(hash string, doCache bool) (map[string][]string, error) { //解码一个节点
	errTimes := 0
	var nodeData *gzip.Reader
	var imageData *bytes.Buffer
	var err error
	if nodeCache == nil {
		nodeCache = make(map[string]*CacheNodeStruct)
	}
	for {
		if data, ok := nodeCache[hash]; ok {
			data.createTime = time.Now().Unix()
			return data.nodeData, nil
		} else {
			imageData, err = GetImage(hash)
			if err != nil {
				runtime.GC()
				return nil, err
			}
		}
		compressedData, err := DecodeImage(imageData)
		if err != nil {
			runtime.GC()
			return nil, err
		}
		nodeData, err = gzip.NewReader(compressedData)
		if err != nil {
			if errTimes <= retryTimes {
				errTimes++
				time.Sleep(retryWaitTime)
				runtime.GC()
				continue
			}
			runtime.GC()
			return nil, err
		}
		break
	}
	defer nodeData.Close()
	data, err := ioutil.ReadAll(nodeData)
	nodeData.Close()
	if err != nil {
		runtime.GC()
		return nil, err
	}
	nodeJsonData := make(map[string][]string)
	err = json.Unmarshal(data, &nodeJsonData)
	if err != nil {
		runtime.GC()
		return nil, err
	}
	if doCache {
		cacheNode := new(CacheNodeStruct)
		cacheNode.nodeData = nodeJsonData
		cacheNode.createTime = time.Now().Unix()
		nodeCache[hash] = cacheNode
	}
	if !nodeCacheManagerStarted {
		go nodeCacheManager()
		nodeCacheManagerStarted = true
	}
	runtime.GC()
	return nodeJsonData, nil
}

func CreateNode(nodeData map[string][]string, doCache bool) (string, error) { //编码一个节点
	if nodeCache == nil {
		nodeCache = make(map[string]*CacheNodeStruct)
	}
	jsonData, err := json.Marshal(nodeData)
	if err != nil {
		runtime.GC()
		return "", err
	}
	compressedData := new(bytes.Buffer)
	writer := gzip.NewWriter(compressedData)
	writer.Write(jsonData)
	writer.Close()
	singleImageMaxSize := (nodeImageWidth*nodeImageMaxHeight - 1) * 4
	if compressedData.Len() > singleImageMaxSize {
		runtime.GC()
		return "", errors.New("single node size is too big")
	}

	imageData, err := EncodeImage(compressedData, nodeImageWidth, nodeImageMaxHeight)
	if err != nil {
		runtime.GC()
		return "", err
	}
	hashByte := sha1.Sum(imageData.Bytes())
	hash := hex.EncodeToString(hashByte[:])
	nodeUploadJobList.PushBack(imageData)

	if doCache {
		cacheNode := new(CacheNodeStruct)
		cacheNode.nodeData = nodeData
		cacheNode.createTime = time.Now().Unix()
		nodeCache[hash] = cacheNode
	}
	if !nodeCacheManagerStarted {
		go nodeCacheManager()
		nodeCacheManagerStarted = true
	}

	runtime.GC()
	return hash, nil
}

func UplaodNodeProcessThread(threadsWaitGroup *sync.WaitGroup) { //上传节点的多线程处理
	for {
		nodeUploadJobListLock.Lock()
		data := nodeUploadJobList.Front()
		if data == nil {
			nodeUploadJobListLock.Unlock()
			break
		}
		nodeUploadJobList.Remove(data)
		nodeUploadJobListLock.Unlock()
		errTimes := 0
		for {
			_, err := PushImage(data.Value.(*bytes.Buffer))
			if err != nil {
				if errTimes <= retryTimes {
					errTimes++
					time.Sleep(retryWaitTime)
					runtime.GC()
					continue
				}
				threadsWaitGroup.Done()
				runtime.GC()
				panic(err)
			}
			break
		}
	}
	threadsWaitGroup.Done()
}

func UploadNode() { //上传已经编码了的节点
	var threadsWaitGroup sync.WaitGroup
	for i := 0; i < nodeUploadThreads; i++ {
		go UplaodNodeProcessThread(&threadsWaitGroup)
		threadsWaitGroup.Add(1)
	}
	threadsWaitGroup.Wait()
}

func InitializeRootNode() (string, error) { //创建根节点
	hash, err := CreateNode(make(map[string][]string), true)
	return hash, err
}

func SetRootNode(hash string) error { //指定根节点
	_, err := DecodeNode(hash, true)
	if err != nil {
		return err
	}
	rootNodeHash = hash
	return nil
}

func GetRootNodeHash() (string, error) { //获取根节点的hash
	if rootNodeHash == "" {
		return "", NotSetARootNodeYet()
	}
	return rootNodeHash, nil
}

func nodeCacheManager() { //超时删除节点缓存
	var nowTime int64
	for {
		nowTime = time.Now().Unix()
		for k, v := range nodeCache {
			if v.createTime+int64(nodeCacheStayTime) >= nowTime {
				delete(nodeCache, k)
			}
		}
		runtime.GC()
		time.Sleep(nodeCacheScanTime)
	}
}
