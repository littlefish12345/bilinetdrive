package bilinetdrive

import (
	"bytes"
	"container/list"
	"io"
	"net"
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

//注意: 以下的路径只能是绝对路径

func UploadProcessDataThread(fileData *bytes.Buffer, blockCount *int64, blockCountLock *sync.Mutex, singleImageMaxSize int64, nodeData *SafeUploadNodeStruct, threadsWaitGroup *sync.WaitGroup) { //上传数据的多线程处理
	for {
		var image *bytes.Buffer
		var err error
		var imageHash string
		stop := false
		errTimes := 0
		blockCountLock.Lock()
		nowCount := *blockCount
		*blockCount = *blockCount + 1
		blockCountLock.Unlock()
		if (nowCount+1)*singleImageMaxSize > int64(fileData.Len()) {
			if nowCount*singleImageMaxSize > int64(fileData.Len()) {
				break
			}
			image, err = EncodeImage(bytes.NewBuffer(fileData.Bytes()[nowCount*singleImageMaxSize:fileData.Len()]), fileImageWidth, fileImageMaxHeight)
			if err != nil {
				runtime.GC()
				threadsWaitGroup.Done()
				panic(err)
			}
			stop = true
		} else {
			image, err = EncodeImage(bytes.NewBuffer(fileData.Bytes()[nowCount*singleImageMaxSize:(nowCount+1)*singleImageMaxSize]), fileImageWidth, fileImageMaxHeight)
			if err != nil {
				runtime.GC()
				threadsWaitGroup.Done()
				panic(err)
			}
		}
		for {
			imageHash, err = PushImage(image)
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
		nodeData.lock.Lock()
		nodeData.mapObj[strconv.FormatInt(nowCount, 10)] = []string{imageHash}
		nodeData.lock.Unlock()
		if stop {
			break
		}
	}
	runtime.GC()
	threadsWaitGroup.Done()
}

func UploadData(path string, data *bytes.Buffer) error { //上传数据 path:目标文件路径
	if rootNodeHash == "" {
		runtime.GC()
		return NotSetARootNodeYet()
	}

	name := GetPathFileName(path)
	folderPath := GetPathFolder(path)
	nodeRWLock.Lock()
	tempPath, err := GetTempPath(folderPath)
	if err != nil {
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	floderNodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	if err != nil {
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	nodeRWLock.Unlock()
	if _, ok := floderNodeData[name]; ok {
		runtime.GC()
		return NameExisted()
	}

	TagFileUsing(path)
	safeNodeData := SafeUploadNodeStruct{new(sync.Mutex), make(map[string][]string)}
	var threadsWaitGroup sync.WaitGroup
	var singleImageMaxSize int64 = (int64(fileImageWidth)*int64(fileImageMaxHeight) - 1) * 4
	var blockCountLock sync.Mutex
	var blockCount int64 = 0
	for i := 0; i < uploadThreads; i++ {
		go UploadProcessDataThread(data, &blockCount, &blockCountLock, singleImageMaxSize, &safeNodeData, &threadsWaitGroup)
		threadsWaitGroup.Add(1)
	}
	threadsWaitGroup.Wait()

	nodeRWLock.Lock()
	hash, err := CreateNode(safeNodeData.mapObj, false)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}

	tempPath, err = GetTempPath(folderPath)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	floderNodeData, err = DecodeNode(tempPath[len(tempPath)-1][1], false)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}

	delete(nodeCache, tempPath[len(tempPath)-1][1])
	floderNodeData[name] = []string{"1", hash, strconv.Itoa(data.Len()), strconv.FormatInt(singleImageMaxSize, 10)}
	lastNodeHash, err := CreateNode(floderNodeData, true)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	tempPath[len(tempPath)-1] = []string{tempPath[len(tempPath)-1][0], lastNodeHash}

	for i := len(tempPath) - 2; i >= 0; i-- {
		nodeData, err := DecodeNode(tempPath[i][1], false)
		delete(nodeCache, tempPath[i][1])
		if err != nil {
			UntagFileUsing(path)
			nodeRWLock.Unlock()
			runtime.GC()
			return err
		}
		nodeData[tempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData, true)
		if err != nil {
			UntagFileUsing(path)
			nodeRWLock.Unlock()
			runtime.GC()
			return err
		}
		tempPath[i] = []string{tempPath[i][0], lastNodeHash}
	}
	rootNodeHash = tempPath[0][1]
	UploadNode()
	UntagFileUsing(path)
	nodeRWLock.Unlock()
	runtime.GC()
	return nil
}

func UploadProcessFileThread(nodeData *SafeUploadNodeStruct, file *os.File, fileLock *sync.Mutex, jobQueue *JobQueueStruct, threadsWaitGroup *sync.WaitGroup) { //上传文件的多线程处理
	for {
		jobQueue.lock.Lock()
		count := jobQueue.list.Front()
		if count == nil {
			jobQueue.lock.Unlock()
			break
		}
		jobQueue.list.Remove(count)
		jobQueue.lock.Unlock()

		var imageHash string
		var singleImageMaxSize int64 = (int64(fileImageWidth)*int64(fileImageMaxHeight) - 1) * 4
		buffer := make([]byte, singleImageMaxSize)
		errTimes := 0
		for {
			fileLock.Lock()
			_, err := file.Seek(singleImageMaxSize*count.Value.(int64), io.SeekStart)
			if err != nil {
				runtime.GC()
				panic(err)
			}
			num, err := file.Read(buffer)
			if err != nil {
				runtime.GC()
				panic(err)
			}
			fileLock.Unlock()
			image, err := EncodeImage(bytes.NewBuffer(buffer[0:num]), fileImageWidth, fileImageMaxHeight)
			if err != nil {
				runtime.GC()
				panic(err)
			}
			imageHash, err = PushImage(image)
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
		nodeData.lock.Lock()
		nodeData.mapObj[strconv.FormatInt(count.Value.(int64), 10)] = []string{imageHash}
		nodeData.lock.Unlock()
	}
	runtime.GC()
	threadsWaitGroup.Done()
}

func UploadFile(file *os.File, path string) error { //上传文件 path:目标文件路径
	if rootNodeHash == "" {
		runtime.GC()
		return NotSetARootNodeYet()
	}

	name := GetPathFileName(path)
	folderPath := GetPathFolder(path)
	nodeRWLock.Lock()
	tempPath, err := GetTempPath(folderPath)
	if err != nil {
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	floderNodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	if err != nil {
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	nodeRWLock.Unlock()
	if _, ok := floderNodeData[name]; ok {
		runtime.GC()
		return NameExisted()
	}

	TagFileUsing(path)
	safeNodeData := SafeUploadNodeStruct{new(sync.Mutex), make(map[string][]string)}
	jobQueue := JobQueueStruct{new(sync.Mutex), list.New()}
	var threadsWaitGroup sync.WaitGroup
	var fileLock sync.Mutex
	var singleImageMaxSize int64 = (int64(fileImageWidth)*int64(fileImageMaxHeight) - 1) * 4
	var nowStartPoint int64 = 0
	var count int64 = 0
	fileStat, err := file.Stat()
	if err != nil {
		UntagFileUsing(path)
		runtime.GC()
		return err
	}
	for {
		jobQueue.list.PushBack(count)
		if nowStartPoint+singleImageMaxSize > fileStat.Size() {
			break
		} else {
			nowStartPoint = nowStartPoint + singleImageMaxSize
			count++
		}
	}
	for i := 0; i < uploadThreads; i++ {
		go UploadProcessFileThread(&safeNodeData, file, &fileLock, &jobQueue, &threadsWaitGroup)
		threadsWaitGroup.Add(1)
	}
	threadsWaitGroup.Wait()

	nodeRWLock.Lock()
	hash, err := CreateNode(safeNodeData.mapObj, false)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}

	tempPath, err = GetTempPath(folderPath)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	floderNodeData, err = DecodeNode(tempPath[len(tempPath)-1][1], false)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}

	delete(nodeCache, tempPath[len(tempPath)-1][1])
	floderNodeData[name] = []string{"1", hash, strconv.FormatInt(fileStat.Size(), 10), strconv.FormatInt(singleImageMaxSize, 10)}
	lastNodeHash, err := CreateNode(floderNodeData, true)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	tempPath[len(tempPath)-1] = []string{tempPath[len(tempPath)-1][0], lastNodeHash}

	for i := len(tempPath) - 2; i >= 0; i-- {
		nodeData, err := DecodeNode(tempPath[i][1], false)
		delete(nodeCache, tempPath[i][1])
		if err != nil {
			UntagFileUsing(path)
			nodeRWLock.Unlock()
			runtime.GC()
			return err
		}
		nodeData[tempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData, true)
		if err != nil {
			UntagFileUsing(path)
			nodeRWLock.Unlock()
			runtime.GC()
			return err
		}
		tempPath[i] = []string{tempPath[i][0], lastNodeHash}
	}
	rootNodeHash = tempPath[0][1]
	UploadNode()
	UntagFileUsing(path)
	nodeRWLock.Unlock()
	runtime.GC()
	return nil
}

func UploadProcessSocketThread(conn *net.Conn, fileLength int64, blockCount *int64, blockCountLock *sync.Mutex, singleImageMaxSize int64, nodeData *SafeUploadNodeStruct, threadsWaitGroup *sync.WaitGroup) { //上传数据的多线程处理
	for {
		var image *bytes.Buffer
		var err error
		var imageHash string
		stop := false
		errTimes := 0
		blockCountLock.Lock()
		nowBlock := *blockCount
		*blockCount = *blockCount + 1
		var recvSize int64 = 0
		if (nowBlock+1)*singleImageMaxSize > int64(fileLength) {
			if nowBlock*singleImageMaxSize > int64(fileLength) {
				break
			}
			recvSize = fileLength - nowBlock*singleImageMaxSize
		} else {
			recvSize = singleImageMaxSize
		}
		buffer := make([]byte, recvSize)

		var totalBytes int64 = 0
		for {
			num, err := (*conn).Read(buffer[totalBytes:recvSize])
			if err != nil && err != io.EOF {
				runtime.GC()
				threadsWaitGroup.Done()
				panic(err)
			}
			totalBytes = totalBytes + int64(num)
			if totalBytes == recvSize {
				break
			}
		}
		blockCountLock.Unlock()

		image, err = EncodeImage(bytes.NewBuffer(buffer), fileImageWidth, fileImageMaxHeight)
		if err != nil {
			runtime.GC()
			threadsWaitGroup.Done()
			panic(err)
		}
		stop = true

		for {
			imageHash, err = PushImage(image)
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
		nodeData.lock.Lock()
		nodeData.mapObj[strconv.FormatInt(nowBlock, 10)] = []string{imageHash}
		nodeData.lock.Unlock()
		if stop {
			break
		}
	}
	runtime.GC()
	threadsWaitGroup.Done()
}

func UploadDataFromSocket(path string, conn *net.Conn, fileLength int64) error { //上传数据 path:目标文件路径
	if rootNodeHash == "" {
		runtime.GC()
		return NotSetARootNodeYet()
	}

	name := GetPathFileName(path)
	folderPath := GetPathFolder(path)
	nodeRWLock.Lock()
	tempPath, err := GetTempPath(folderPath)
	if err != nil {
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	floderNodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	if err != nil {
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	nodeRWLock.Unlock()
	if _, ok := floderNodeData[name]; ok {
		runtime.GC()
		return NameExisted()
	}

	TagFileUsing(path)
	safeNodeData := SafeUploadNodeStruct{new(sync.Mutex), make(map[string][]string)}
	var threadsWaitGroup sync.WaitGroup
	var singleImageMaxSize int64 = (int64(fileImageWidth)*int64(fileImageMaxHeight) - 1) * 4
	var blockCountLock sync.Mutex
	var blockCount int64 = 0
	for i := 0; i < uploadThreads; i++ {
		go UploadProcessSocketThread(conn, fileLength, &blockCount, &blockCountLock, singleImageMaxSize, &safeNodeData, &threadsWaitGroup)
		threadsWaitGroup.Add(1)
	}
	threadsWaitGroup.Wait()

	nodeRWLock.Lock()
	hash, err := CreateNode(safeNodeData.mapObj, false)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}

	tempPath, err = GetTempPath(folderPath)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	floderNodeData, err = DecodeNode(tempPath[len(tempPath)-1][1], false)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}

	delete(nodeCache, tempPath[len(tempPath)-1][1])
	floderNodeData[name] = []string{"1", hash, strconv.FormatInt(fileLength, 10), strconv.FormatInt(singleImageMaxSize, 10)}
	lastNodeHash, err := CreateNode(floderNodeData, true)
	if err != nil {
		UntagFileUsing(path)
		nodeRWLock.Unlock()
		runtime.GC()
		return err
	}
	tempPath[len(tempPath)-1] = []string{tempPath[len(tempPath)-1][0], lastNodeHash}

	for i := len(tempPath) - 2; i >= 0; i-- {
		nodeData, err := DecodeNode(tempPath[i][1], false)
		delete(nodeCache, tempPath[i][1])
		if err != nil {
			UntagFileUsing(path)
			nodeRWLock.Unlock()
			runtime.GC()
			return err
		}
		nodeData[tempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData, true)
		if err != nil {
			UntagFileUsing(path)
			nodeRWLock.Unlock()
			runtime.GC()
			return err
		}
		tempPath[i] = []string{tempPath[i][0], lastNodeHash}
	}
	rootNodeHash = tempPath[0][1]
	UploadNode()
	UntagFileUsing(path)
	nodeRWLock.Unlock()
	runtime.GC()
	return nil
}
