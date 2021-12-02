package bilinetdrive

import (
	"bytes"
	"container/list"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

/*
[[filename, hash], ...] 内部路径
{filename:[type, hash, (type=1)length, (type=1)blockSize], ...} node文件夹
{partnum:[hash], ...} node文件
type: 0是文件夹 1是文件
*/

//注意: 以下的path只能是绝对路径
func DownloadProcessDataThread(writeMap *SafeDownloadMapStruct, jobQueue *JobQueueStruct, threadsWaitGroup *sync.WaitGroup) { //下载数据的多线程处理
	for {
		jobQueue.lock.Lock()
		downloadData := jobQueue.list.Front()
		if downloadData == nil {
			jobQueue.lock.Unlock()
			break
		}
		jobQueue.list.Remove(downloadData)
		jobQueue.lock.Unlock()

		var data *bytes.Buffer
		var imageData *bytes.Buffer
		var err error
		errTimes := 0
		for {
			imageData, err = GetImage(downloadData.Value.([]string)[1])
			if err != nil {
				if errTimes <= retryTimes {
					errTimes++
					time.Sleep(retryWaitTime)
					runtime.GC()
					continue
				}
				runtime.GC()
				threadsWaitGroup.Done()
				panic(err)
			}
			break
		}
		data, err = DecodeImage(imageData)
		if err != nil {
			threadsWaitGroup.Done()
			runtime.GC()
			panic(err)
		}

		num, err := strconv.ParseInt(downloadData.Value.([]string)[0], 10, 64)
		if err != nil {
			threadsWaitGroup.Done()
			runtime.GC()
			panic(err)
		}

		writeMap.lock.Lock()
		writeMap.mapObj[num] = data.Bytes()
		writeMap.lock.Unlock()
	}
	threadsWaitGroup.Done()
}

func DownloadData(path string) (*bytes.Buffer, error) { //从当前文件夹下载数据 path:目标文件路径 返回值:文件的*bytes.Buffer
	if rootNodeHash == "" {
		runtime.GC()
		return nil, NotSetARootNodeYet()
	}

	name := GetPathFileName(path)
	nodeRWLock.RLock()
	tempPath, err := GetTempPath(GetPathFolder(path))
	if err != nil {
		nodeRWLock.RUnlock()
		runtime.GC()
		return nil, err
	}
	nodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	if err != nil {
		nodeRWLock.RUnlock()
		runtime.GC()
		return nil, err
	}
	nodeRWLock.RUnlock()
	if folderNodeData, ok := nodeData[name]; ok {
		if folderNodeData[0] == "1" {
			nodeRWLock.RLock()
			fileNodeData, err := DecodeNode(folderNodeData[1], false)
			nodeRWLock.RUnlock()
			if err != nil {
				runtime.GC()
				return nil, err
			}
			writeMap := SafeDownloadMapStruct{new(sync.Mutex), make(map[int64][]byte)}
			jobQueue := JobQueueStruct{new(sync.Mutex), list.New()}
			var threadsWaitGroup sync.WaitGroup
			var k string
			for i := 0; i < len(fileNodeData); i++ {
				k = strconv.Itoa(i)
				jobQueue.list.PushBack([]string{k, fileNodeData[k][0]})
			}
			for i := 0; i < downloadThreads; i++ {
				go DownloadProcessDataThread(&writeMap, &jobQueue, &threadsWaitGroup)
				threadsWaitGroup.Add(1)
			}
			threadsWaitGroup.Wait()
			fileData := new(bytes.Buffer)
			var i int64
			for i = 0; ; i++ {
				if _, ok := writeMap.mapObj[i]; !ok {
					break
				}
				fileData.Write(writeMap.mapObj[i])
			}
			runtime.GC()
			return fileData, nil
		} else {
			runtime.GC()
			return nil, FileDoesNotExist()
		}
	}
	runtime.GC()
	return nil, FileDoesNotExist()
}

func DownloadProcessFileThread(file *os.File, fileLock *sync.Mutex, blockSize int64, jobQueue *JobQueueStruct, threadsWaitGroup *sync.WaitGroup) { //下载数据的多线程处理
	for {
		jobQueue.lock.Lock()
		downloadData := jobQueue.list.Front()
		if downloadData == nil {
			jobQueue.lock.Unlock()
			break
		}
		jobQueue.list.Remove(downloadData)
		jobQueue.lock.Unlock()

		var data *bytes.Buffer
		var imageData *bytes.Buffer
		var err error
		errTimes := 0
		for {
			imageData, err = GetImage(downloadData.Value.([]string)[1])
			if err != nil {
				if errTimes <= retryTimes {
					errTimes++
					time.Sleep(retryWaitTime)
					runtime.GC()
					continue
				}
				runtime.GC()
				threadsWaitGroup.Done()
				panic(err)
			}
			break
		}
		data, err = DecodeImage(imageData)
		if err != nil {
			threadsWaitGroup.Done()
			runtime.GC()
			panic(err)
		}

		count, err := strconv.ParseInt(downloadData.Value.([]string)[0], 10, 64)
		if err != nil {
			threadsWaitGroup.Done()
			runtime.GC()
			panic(err)
		}

		fileLock.Lock()
		_, err = file.Seek(blockSize*count, io.SeekStart)
		if err != nil {
			threadsWaitGroup.Done()
			runtime.GC()
			panic(err)
		}
		_, err = file.Write(data.Bytes())
		if err != nil {
			threadsWaitGroup.Done()
			runtime.GC()
			panic(err)
		}
		fileLock.Unlock()
	}
	threadsWaitGroup.Done()
}

func DownloadFile(path string, file *os.File) (int64, error) { //从当前文件夹下载数据 path:目标文件路径 返回值:文件长度
	if rootNodeHash == "" {
		runtime.GC()
		return 0, NotSetARootNodeYet()
	}

	name := GetPathFileName(path)
	nodeRWLock.RLock()
	tempPath, err := GetTempPath(GetPathFolder(path))
	if err != nil {
		nodeRWLock.RUnlock()
		runtime.GC()
		return 0, err
	}
	nodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	if err != nil {
		nodeRWLock.RUnlock()
		runtime.GC()
		return 0, err
	}
	nodeRWLock.RUnlock()
	if folderNodeData, ok := nodeData[name]; ok {
		if folderNodeData[0] == "1" {
			nodeRWLock.RLock()
			fileNodeData, err := DecodeNode(folderNodeData[1], false)
			nodeRWLock.RUnlock()
			if err != nil {
				runtime.GC()
				return 0, err
			}
			jobQueue := JobQueueStruct{new(sync.Mutex), list.New()}
			var threadsWaitGroup sync.WaitGroup
			var fileLock sync.Mutex
			blockSize, err := strconv.ParseInt(folderNodeData[3], 10, 64)
			if err != nil {
				runtime.GC()
				return 0, err
			}
			fileSize, err := strconv.ParseInt(folderNodeData[2], 10, 64)
			if err != nil {
				runtime.GC()
				return 0, err
			}
			var k string
			for i := 0; i < len(fileNodeData); i++ {
				k = strconv.Itoa(i)
				jobQueue.list.PushBack([]string{k, fileNodeData[k][0]})
			}
			for i := 0; i < downloadThreads; i++ {
				go DownloadProcessFileThread(file, &fileLock, blockSize, &jobQueue, &threadsWaitGroup)
				threadsWaitGroup.Add(1)
			}
			threadsWaitGroup.Wait()
			runtime.GC()
			return fileSize, nil
		} else {
			runtime.GC()
			return 0, FileDoesNotExist()
		}
	}
	runtime.GC()
	return 0, FileDoesNotExist()
}
