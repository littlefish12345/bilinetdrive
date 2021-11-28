package bilinetdrive

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"io/ioutil"
	"math"
	"mime/multipart"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	nodeImageWidth     = 512
	nodeImageMaxHeight = 9000
	fileImageWidth     = 512
	fileImageMaxHeight = 512
	retryTimes         = 10
	uploadThreads      = 16
	downloadThreads    = 32
)

var (
	SESSDATA   = ""
	Path       = [][]string{}
	UserAgent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0"
	httpClient = http.Client{Timeout: 5 * time.Second}
)

/*
[[filename, hash], ...] 内部路径
{filename:[type, hash, (type=1)length], ...} node文件夹
{partnum:[hash], ...} node文件
type: 0是文件夹 1是文件 2是软链接
*/

type JobQueueStruct struct {
	lock *sync.Mutex
	list *list.List
}

type SafeDownloadMapStruct struct {
	lock   *sync.Mutex
	mapObj map[int][]byte
}

type SafeUploadNodeStruct struct {
	lock   *sync.Mutex
	mapObj map[string][]string
}

func NotSetARootNodeYet() error {
	return errors.New("Not set a root node yet")
}

func PathDoesNotExist() error {
	return errors.New("Path does not exists")
}

func FolderDoesNotExist() error {
	return errors.New("Folder does not exists")
}

func FileDoesNotExist() error {
	return errors.New("File does not exists")
}

func NotAFile() error {
	return errors.New("Not A File")
}

func NodeDoesNotExist() error {
	return errors.New("Node does not exists")
}

func NameExisted() error {
	return errors.New("Name Existed")
}

func SetSESSDATA(sessdata string) { //要修改或上传必须要有
	SESSDATA = sessdata
}

func CostumeUserAgent(useragent string) { //自定义UA
	UserAgent = useragent
}

func DecodeImage(imageData *bytes.Buffer) (*bytes.Buffer, error) { //解码图像
	fileImage, err := png.Decode(imageData)
	if err != nil {
		runtime.GC()
		return nil, err
	}
	dataBuffer := new(bytes.Buffer)
	colorBuffer := make([]byte, 4)
	pointColor := color.NRGBAModel.Convert(fileImage.At(fileImage.Bounds().Min.X, fileImage.Bounds().Min.Y)).(color.NRGBA)
	colorBuffer = []byte{pointColor.R, pointColor.G, pointColor.B, pointColor.A}
	num := uint32(binary.BigEndian.Uint32(colorBuffer))
	var readLength uint32
	for k := fileImage.Bounds().Min.Y; k < fileImage.Bounds().Max.Y; k++ {
		for j := fileImage.Bounds().Min.X; j < fileImage.Bounds().Max.X; j++ {
			if k == 0 && j == 0 {
				j = 1
			}
			readLength = uint32(k*fileImage.Bounds().Dx()+j)*4 - 4
			if readLength+4 > num {
				outNum := num - readLength
				if outNum == 1 {
					pointColor = color.NRGBAModel.Convert(fileImage.At(j, k)).(color.NRGBA)
					dataBuffer.Write([]byte{pointColor.R})
				} else if outNum == 2 {
					pointColor = color.NRGBAModel.Convert(fileImage.At(j, k)).(color.NRGBA)
					dataBuffer.Write([]byte{pointColor.R, pointColor.G})
				} else if outNum == 3 {
					pointColor = color.NRGBAModel.Convert(fileImage.At(j, k)).(color.NRGBA)
					dataBuffer.Write([]byte{pointColor.R, pointColor.G, pointColor.B})
				}
				goto loopOut
			} else {
				pointColor = color.NRGBAModel.Convert(fileImage.At(j, k)).(color.NRGBA)
				dataBuffer.Write([]byte{pointColor.R, pointColor.G, pointColor.B, pointColor.A})
			}
		}
	}
loopOut:
	imageData.Reset()
	runtime.GC()
	return dataBuffer, nil
}

func EncodeImage(data *bytes.Buffer, imageWidth int, imageMaxHeight int, startPos int) (*bytes.Buffer, error) { //编码图像
	if imageWidth < 10 {
		imageWidth = 10
	}
	if imageMaxHeight < 10 {
		imageMaxHeight = 10
	}
	singleImageMaxSize := (imageWidth*imageMaxHeight - 1) * 4
	var buffer []byte

	if startPos+singleImageMaxSize > data.Len() {
		buffer = data.Bytes()[startPos:data.Len()]
	} else {
		buffer = data.Bytes()[startPos : startPos+singleImageMaxSize]
	}
	num := len(buffer)

	var imageHeight int
	if num < singleImageMaxSize {
		imageHeight = int(math.Ceil(float64(num+4) / float64(imageWidth*4)))
		if imageHeight < 10 {
			imageHeight = 10
		}
	} else {
		imageHeight = imageMaxHeight
	}
	blockImage := image.NewNRGBA(image.Rect(0, 0, imageWidth, imageHeight))
	colorBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(colorBuf, uint32(num))
	blockImage.SetNRGBA(0, 0, color.NRGBA{colorBuf[0], colorBuf[1], colorBuf[2], colorBuf[3]})
	var pointer uint32
	for i := 0; i < imageHeight; i++ {
		for j := 0; j < imageWidth; j++ {
			if i == 0 && j == 0 {
				j = 1
			}
			pointer = uint32(i*imageWidth+j)*4 - 4
			if pointer+4 > uint32(num) {
				outNum := uint32(num) - pointer
				if outNum == 1 {
					blockImage.SetNRGBA(j, i, color.NRGBA{buffer[pointer], 0, 0, 0})
				} else if outNum == 2 {
					blockImage.SetNRGBA(j, i, color.NRGBA{buffer[pointer], buffer[pointer+1], 0, 0})
				} else if outNum == 3 {
					blockImage.SetNRGBA(j, i, color.NRGBA{buffer[pointer], buffer[pointer+1], buffer[pointer+2], 0})
				}
				goto loopOut
			} else {
				blockImage.SetNRGBA(j, i, color.NRGBA{buffer[pointer], buffer[pointer+1], buffer[pointer+2], buffer[pointer+3]})
			}
		}
	}
loopOut:
	imageData := new(bytes.Buffer)
	png.Encode(imageData, blockImage)

	runtime.GC()
	return imageData, nil
}

func GetImage(hash string) (*bytes.Buffer, error) { //获取图片
	request, err := http.NewRequest("GET", "http://i0.hdslb.com/bfs/album/"+hash+".png", nil)
	if err != nil {
		runtime.GC()
		return nil, err
	}
	request.Header.Add("Cookie", "SESSDATA="+SESSDATA)
	request.Header.Add("User-Agent", UserAgent)
	request.Header.Add("Origin", "https://t.bilibili.com")
	request.Header.Add("Referer", "https://t.bilibili.com")
	response, err := httpClient.Do(request)
	if err != nil {
		runtime.GC()
		return nil, err
	}
	defer response.Body.Close()
	imageData, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
	runtime.GC()
	return bytes.NewBuffer(imageData), nil
}

func PushImage(imageData *bytes.Buffer) (string, error) { //上传图片
	body := new(bytes.Buffer)
	postData := multipart.NewWriter(body)
	part1, err := postData.CreateFormFile("file_up", "file.png")
	_, err = io.Copy(part1, imageData)
	if err != nil {
		runtime.GC()
		return "", err
	}
	postData.WriteField("biz", "draw")
	postData.WriteField("category", "daily")
	err = postData.Close()
	if err != nil {
		runtime.GC()
		return "", err
	}
	request, err := http.NewRequest("POST", "https://api.vc.bilibili.com/api/v1/drawImage/upload", body)
	if err != nil {
		runtime.GC()
		return "", err
	}
	request.Header.Add("Cookie", "SESSDATA="+SESSDATA)
	request.Header.Add("User-Agent", UserAgent)
	request.Header.Add("Origin", "https://t.bilibili.com")
	request.Header.Add("Referer", "https://t.bilibili.com")
	request.Header.Add("Content-Type", postData.FormDataContentType())
	response, err := httpClient.Do(request)
	if err != nil {
		runtime.GC()
		return "", err
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		runtime.GC()
		return "", err
	}
	jsonData := make(map[string]interface{})
	err = json.Unmarshal(data, &jsonData)
	if err != nil {
		runtime.GC()
		return "", err
	}
	if v, ok := jsonData["code"].(float64); ok {
		if v != 0 {
			return "", errors.New("Upload Failure: " + strconv.Itoa(int(v)) + " " + jsonData["message"].(string))
		} else {
			if v, ok := jsonData["data"].(map[string]interface{}); ok {
				if url, ok := v["image_url"].(string); ok {
					urlSplit := strings.Split(url, "/")
					runtime.GC()
					return strings.Split(urlSplit[len(urlSplit)-1], ".")[0], nil
				}
			}
		}
	}
	runtime.GC()
	return "", errors.New("Upload Failure: error json format")
}

func DecodeNode(hash string) (map[string][]string, error) { //解码一个节点 nodeType
	errTimes := 0
	var nodeData *gzip.Reader
	for {
		imageData, err := GetImage(hash)
		compressedData, err := DecodeImage(imageData)
		nodeData, err = gzip.NewReader(compressedData)
		if err != nil {
			if errTimes <= retryTimes {
				errTimes++
				time.Sleep(time.Millisecond * 500)
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
	runtime.GC()
	return nodeJsonData, nil
}

func CreateNode(nodeData map[string][]string) (string, error) { //创建一个节点
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
		return "", errors.New("Single node size is too big")
	}

	var hash string
	errTimes := 0
	for {
		imageData, err := EncodeImage(compressedData, nodeImageWidth, nodeImageMaxHeight, 0)
		hash, err = PushImage(imageData)
		if err != nil {
			if errTimes <= retryTimes {
				errTimes++
				time.Sleep(time.Millisecond * 500)
				runtime.GC()
				continue
			}
			runtime.GC()
			return "", err
		}
		break
	}
	runtime.GC()
	return hash, nil
}

func InitializeRootNode() (string, error) { //创建根节点
	hash, err := CreateNode(map[string][]string{})
	return hash, err
}

func SetRootNode(hash string) error { //指定根节点
	_, err := DecodeNode(hash)
	if err != nil {
		return err
	}
	Path = [][]string{{"root", hash}}
	return nil
}

func GetRootNodeHash() (string, error) { //获取根节点的hash
	if len(Path) == 0 {
		return "", NotSetARootNodeYet()
	}
	return Path[0][1], nil
}

func GetPwd() (string, error) { //获取当前路径(pwd)
	if len(Path) == 0 {
		return "", NotSetARootNodeYet()
	}
	currentPath := "/"
	for i := 1; i < len(Path); i++ {
		currentPath = currentPath + Path[i][0] + "/"
	}
	return currentPath, nil
}

func ListFile() ([][]string, error) { //获取当前文件夹下所有东西(ls)
	if len(Path) == 0 {
		return nil, NotSetARootNodeYet()
	}
	var fileList [][]string
	fileMap, err := DecodeNode(Path[len(Path)-1][1])
	if err != nil {
		return nil, err
	}
	for k, v := range fileMap {
		fileList = append(fileList, []string{k, v[0]})
	}
	return fileList, nil
}

func SwitchDir(path string) error { //更改路径(cd)
	if len(Path) == 0 {
		return NotSetARootNodeYet()
	}
	pathList := strings.Split(path, "/")
	for i := 0; i < len(pathList); i++ {
		if pathList[i] == "." {
			continue
		} else if pathList[i] == "" && i == 0 {
			Path = [][]string{Path[0]}
			if len(pathList) == 2 {
				if pathList[1] == "" {
					break
				}
			}
		} else if pathList[i] == ".." {
			if len(Path) > 1 {
				Path = Path[0 : len(Path)-1]
			} else {
				return PathDoesNotExist()
			}
		} else {
			nodeType := ""
			hash := ""
			fileMap, err := DecodeNode(Path[len(Path)-1][1])
			if err != nil {
				return err
			}
			for k, v := range fileMap {
				if k == pathList[i] {
					nodeType = v[0]
					hash = v[1]
					goto NodeFound
				}
			}
			return NodeDoesNotExist()
		NodeFound:
			if err != nil {
				if err == NodeDoesNotExist() {
					return FolderDoesNotExist()
				} else {
					return err
				}
			}
			if nodeType == "0" {
				Path = append(Path, []string{pathList[i], hash})
			} else if nodeType == "1" {
				return FolderDoesNotExist()
			}
		}
	}
	return nil
}

func MakeFolder(name string) error { //在当前目录下创建一个文件夹(mkdir)
	if len(Path) == 0 {
		return NotSetARootNodeYet()
	}
	hash, err := CreateNode(map[string][]string{})
	if err != nil {
		return err
	}

	nodeData, err := DecodeNode(Path[len(Path)-1][1])
	if err != nil {
		return err
	}
	if _, ok := nodeData[name]; ok {
		return NameExisted()
	}
	nodeData[name] = []string{"0", hash}
	lastNodeHash, err := CreateNode(nodeData)
	if err != nil {
		return err
	}
	Path[len(Path)-1] = []string{Path[len(Path)-1][0], lastNodeHash}

	for i := len(Path) - 2; i >= 0; i-- {
		nodeData, err = DecodeNode(Path[i][1])
		if err != nil {
			return err
		}
		nodeData[Path[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData)
		if err != nil {
			return err
		}
		Path[i] = []string{Path[len(Path)-1][0], lastNodeHash}
	}
	return nil
}

func RemoveNode(name string) error { //删除当前文件夹下的一个文件或文件夹(rm)
	if len(Path) == 0 {
		return NotSetARootNodeYet()
	}

	nodeData, err := DecodeNode(Path[len(Path)-1][1])
	if err != nil {
		return err
	}
	if _, ok := nodeData[name]; !ok {
		return NodeDoesNotExist()
	}
	delete(nodeData, name)
	lastNodeHash, err := CreateNode(nodeData)
	if err != nil {
		return err
	}
	Path[len(Path)-1] = []string{Path[len(Path)-1][0], lastNodeHash}

	for i := len(Path) - 2; i >= 0; i-- {
		nodeData, err = DecodeNode(Path[i][1])
		if err != nil {
			return err
		}
		nodeData[Path[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData)
		if err != nil {
			return err
		}
		Path[i] = []string{Path[len(Path)-1][0], lastNodeHash}
	}
	return nil
}

func RenameNode(origin string, name string) error { //重命名当前目录下的一个文件或文件夹
	if len(Path) == 0 {
		return NotSetARootNodeYet()
	}

	nodeData, err := DecodeNode(Path[len(Path)-1][1])
	if err != nil {
		return err
	}
	if _, ok := nodeData[origin]; !ok {
		return NodeDoesNotExist()
	}
	if _, ok := nodeData[name]; ok {
		return NameExisted()
	}
	nodeData[name] = nodeData[origin]
	delete(nodeData, origin)
	lastNodeHash, err := CreateNode(nodeData)
	if err != nil {
		return err
	}
	Path[len(Path)-1] = []string{Path[len(Path)-1][0], lastNodeHash}

	for i := len(Path) - 2; i >= 0; i-- {
		nodeData, err = DecodeNode(Path[i][1])
		if err != nil {
			return err
		}
		nodeData[Path[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData)
		if err != nil {
			return err
		}
		Path[i] = []string{Path[len(Path)-1][0], lastNodeHash}
	}
	return nil
}

func GetFileLength(name string) (int, error) {
	if len(Path) == 0 {
		return 0, NotSetARootNodeYet()
	}

	nodeData, err := DecodeNode(Path[len(Path)-1][1])
	if err != nil {
		return 0, err
	}
	if _, ok := nodeData[name]; !ok {
		return 0, NodeDoesNotExist()
	}
	if nodeData[name][0] == "1" {
		return strconv.Atoi(nodeData[name][2])
	}
	return 0, NotAFile()
}

func UploadProcessFileThread(nodeData *SafeUploadNodeStruct, data *bytes.Buffer, jobQueue *JobQueueStruct, threadsWaitGroup *sync.WaitGroup) { //上传文件的多线程处理
	for {
		jobQueue.lock.Lock()
		startPos := jobQueue.list.Front()
		if startPos == nil {
			jobQueue.lock.Unlock()
			break
		}
		jobQueue.list.Remove(startPos)
		jobQueue.lock.Unlock()

		var imageHash string
		errTimes := 0
		for {
			image, err := EncodeImage(data, fileImageWidth, fileImageMaxHeight, startPos.Value.([]int)[1])
			imageHash, err = PushImage(image)
			if err != nil {
				if errTimes <= retryTimes {
					errTimes++
					time.Sleep(time.Millisecond * 500)
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
		nodeData.mapObj[strconv.Itoa(startPos.Value.([]int)[0])] = []string{imageHash}
		nodeData.lock.Unlock()
	}
	runtime.GC()
	threadsWaitGroup.Done()
}

func UploadFile(data *bytes.Buffer, name string) error { //上传文件到当前路径
	if len(Path) == 0 {
		runtime.GC()
		return NotSetARootNodeYet()
	}

	floderNodeData, err := DecodeNode(Path[len(Path)-1][1])
	if err != nil {
		runtime.GC()
		return err
	}
	if _, ok := floderNodeData[name]; ok {
		runtime.GC()
		return NameExisted()
	}

	safeNodeData := SafeUploadNodeStruct{new(sync.Mutex), make(map[string][]string)}
	jobQueue := JobQueueStruct{new(sync.Mutex), list.New()}
	var threadsWaitGroup sync.WaitGroup
	singleImageMaxSize := (fileImageWidth*fileImageMaxHeight - 1) * 4
	nowStartPoint := 0
	count := 0
	for {
		jobQueue.list.PushBack([]int{count, nowStartPoint})
		nowStartPoint = nowStartPoint + singleImageMaxSize
		if nowStartPoint > data.Len() {
			break
		}
		count++
	}
	for i := 0; i < uploadThreads; i++ {
		go UploadProcessFileThread(&safeNodeData, data, &jobQueue, &threadsWaitGroup)
		threadsWaitGroup.Add(1)
	}
	threadsWaitGroup.Wait()

	hash, err := CreateNode(safeNodeData.mapObj)
	if err != nil {
		runtime.GC()
		return err
	}

	floderNodeData[name] = []string{"1", hash, strconv.Itoa(data.Len())}
	lastNodeHash, err := CreateNode(floderNodeData)
	if err != nil {
		runtime.GC()
		return err
	}
	Path[len(Path)-1] = []string{Path[len(Path)-1][0], lastNodeHash}

	nodeData := make(map[string][]string)
	for i := len(Path) - 2; i >= 0; i-- {
		nodeData, err = DecodeNode(Path[i][1])
		if err != nil {
			runtime.GC()
			return err
		}
		nodeData[Path[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData)
		if err != nil {
			runtime.GC()
			return err
		}
		Path[i] = []string{Path[len(Path)-1][0], lastNodeHash}
	}
	runtime.GC()
	return nil
}

func DownloadProcessFileThread(writeMap *SafeDownloadMapStruct, jobQueue *JobQueueStruct, threadsWaitGroup *sync.WaitGroup) { //下载文件的多线程处理
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
		errTimes := 0
		for {
			imageData, err := GetImage(downloadData.Value.([]string)[1])
			data, err = DecodeImage(imageData)
			if err != nil {
				if errTimes <= retryTimes {
					errTimes++
					time.Sleep(time.Millisecond * 500)
					runtime.GC()
					continue
				}
				runtime.GC()
				threadsWaitGroup.Done()
				panic(err)
			}
			break
		}

		num, err := strconv.Atoi(downloadData.Value.([]string)[0])
		if err != nil {
			panic(err)
		}

		jobQueue.lock.Lock()
		writeMap.mapObj[num] = data.Bytes()
		jobQueue.lock.Unlock()
	}
	threadsWaitGroup.Done()
}

func DownloadFile(filename string) (*bytes.Buffer, error) { //从当前文件夹下载文件
	if len(Path) == 0 {
		runtime.GC()
		return nil, NotSetARootNodeYet()
	}

	nodeData, err := DecodeNode(Path[len(Path)-1][1])
	if err != nil {
		runtime.GC()
		return nil, err
	}
	if folderNodeData, ok := nodeData[filename]; ok {
		if folderNodeData[0] == "1" {
			fileNodeData, err := DecodeNode(folderNodeData[1])
			if err != nil {
				runtime.GC()
				return nil, err
			}
			writeMap := SafeDownloadMapStruct{new(sync.Mutex), make(map[int][]byte)}
			jobQueue := JobQueueStruct{new(sync.Mutex), list.New()}
			var threadsWaitGroup sync.WaitGroup
			for k, v := range fileNodeData {
				jobQueue.list.PushBack([]string{k, v[0]})
			}
			for i := 0; i < downloadThreads; i++ {
				go DownloadProcessFileThread(&writeMap, &jobQueue, &threadsWaitGroup)
				threadsWaitGroup.Add(1)
			}
			threadsWaitGroup.Wait()
			fileData := new(bytes.Buffer)
			for i := 0; ; i++ {
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

/*
[[filename, hash], ...] 内部路径
{filename:[type, hash, (type=1)length], ...} node文件夹
{partnum:[hash], ...} node文件
type: 0是文件夹 1是文件 2是软链接
*/
