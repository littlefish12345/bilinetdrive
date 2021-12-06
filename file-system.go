package bilinetdrive

import (
	"strconv"
	"strings"
)

/*
[[filename, hash], ...] 内部路径
{filename:[type, hash, (type=1)length, (type=1)blockSize], ...} node文件夹
{partnum:[hash], ...} node文件
type: 0是文件夹 1是文件
*/

//注意: 以下的路径只能是绝对路径

func IsFileUsing(path string) bool {
	fileNowUsingListLock.Lock()
	for v := fileNowUsingList.Front(); v != nil; v = v.Next() {
		splitList := strings.Split(v.Value.(string), path)
		if len(splitList) >= 2 {
			if splitList[0] == "" {
				fileNowUsingListLock.Unlock()
				return true
			}
		}
	}
	fileNowUsingListLock.Unlock()
	return false
}

func TagFileUsing(path string) {
	fileNowUsingListLock.Lock()
	fileNowUsingList.PushBack(path)
	fileNowUsingListLock.Unlock()
}

func UntagFileUsing(path string) {
	fileNowUsingListLock.Lock()
	for v := fileNowUsingList.Front(); v != nil; v = v.Next() {
		if v.Value.(string) == path {
			fileNowUsingList.Remove(v)
			break
		}
	}
	fileNowUsingListLock.Unlock()
}

func GetTempPath(path string) ([][]string, error) { //获取内部path列表
	if rootNodeHash == "" {
		return nil, NotSetARootNodeYet()
	}

	pathList := strings.Split(path, "/")
	if pathList[len(pathList)-1] == "" && len(pathList) >= 3 {
		pathList = pathList[:len(pathList)-1]
	}
	pathList = pathList[1:]
	tempPath := [][]string{{"root", rootNodeHash}}
	var hash string
	var nodeType string
	if pathList[0] == "" && len(pathList) == 1 {
		return tempPath, nil
	}
	for i := 0; i < len(pathList); i++ {
		fileMap, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
		if err != nil {
			return nil, err
		}
		for k, v := range fileMap {
			if k == pathList[i] {
				nodeType = v[0]
				hash = v[1]
				goto NodeFound
			}
		}
		return nil, FolderDoesNotExist()
	NodeFound:
		if nodeType == "0" {
			tempPath = append(tempPath, []string{pathList[i], hash})
		} else {
			return nil, FolderDoesNotExist()
		}
	}
	return tempPath, nil
}

func DoesNodeExist(path string) (bool, string, error) { //查看节点是否存在 path:要查询的路径 返回值:是否存在, node类型
	if rootNodeHash == "" {
		return false, "", NotSetARootNodeYet()
	}
	if path == "/" {
		return true, "0", nil
	}

	nodeRWLock.Lock()
	tempPath, err := GetTempPath(GetPathFolder(path))
	if err != nil {
		nodeRWLock.Unlock()
		return false, "", err
	}
	nodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	nodeRWLock.Unlock()
	if err != nil {
		return false, "", err
	}

	if fileData, ok := nodeData[GetPathFileName(path)]; ok {
		return true, fileData[0], nil
	}
	return false, "", nil
}

func ListFile(path string) ([][]string, error) { //获取当前文件夹下所有东西(ls) path:要获取的路径 返回值:[[文件名,node类型], ...]
	if rootNodeHash == "" {
		return nil, NotSetARootNodeYet()
	}
	var fileList [][]string
	nodeRWLock.Lock()
	tempPath, err := GetTempPath(path)
	if err != nil {
		return nil, err
	}
	fileMap, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	nodeRWLock.Unlock()
	if err != nil {
		return nil, err
	}
	for k, v := range fileMap {
		fileList = append(fileList, []string{k, v[0]})
	}
	return fileList, nil
}

func MakeFolder(path string) error { //创建一个文件夹(mkdir) path:要创建文件夹的路径
	if rootNodeHash == "" {
		return NotSetARootNodeYet()
	}
	hash, err := CreateNode(make(map[string][]string), true)
	if err != nil {
		return err
	}

	name := GetPathFileName(path)
	nodeRWLock.Lock()
	tempPath, err := GetTempPath(GetPathFolder(path))
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	nodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], false)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	if _, ok := nodeData[name]; ok {
		nodeRWLock.Unlock()
		return NameExisted()
	}
	delete(nodeCache, tempPath[len(tempPath)-1][1])
	nodeData[name] = []string{"0", hash}
	lastNodeHash, err := CreateNode(nodeData, true)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	tempPath[len(tempPath)-1] = []string{tempPath[len(tempPath)-1][0], lastNodeHash}

	for i := len(tempPath) - 2; i >= 0; i-- {
		nodeData, err = DecodeNode(tempPath[i][1], false)
		delete(nodeCache, tempPath[i][1])
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		nodeData[tempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData, true)
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		tempPath[i] = []string{tempPath[i][0], lastNodeHash}
	}
	rootNodeHash = tempPath[0][1]
	UploadNode()
	nodeRWLock.Unlock()
	return nil
}

func RemoveNode(path string) error { //删除一个文件或文件夹(rm) path:要删除的文件或文件夹的路径
	if rootNodeHash == "" {
		return NotSetARootNodeYet()
	}

	if IsFileUsing(path) {
		return FileIsUsing()
	}
	name := GetPathFileName(path)
	nodeRWLock.Lock()
	tempPath, err := GetTempPath(GetPathFolder(path))
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	nodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], false)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	if _, ok := nodeData[name]; !ok {
		nodeRWLock.Unlock()
		return NodeDoesNotExist()
	}
	delete(nodeCache, tempPath[len(tempPath)-1][1])
	delete(nodeData, name)
	lastNodeHash, err := CreateNode(nodeData, true)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	tempPath[len(tempPath)-1] = []string{tempPath[len(tempPath)-1][0], lastNodeHash}

	for i := len(tempPath) - 2; i >= 0; i-- {
		nodeData, err = DecodeNode(tempPath[i][1], false)
		delete(nodeCache, tempPath[i][1])
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		nodeData[tempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(nodeData, true)
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		tempPath[i] = []string{tempPath[i][0], lastNodeHash}
	}
	rootNodeHash = tempPath[0][1]
	UploadNode()
	nodeRWLock.Unlock()
	return nil
}

func MoveNode(originPath string, targetPath string) error { //移动一个文件或文件夹(mv) originPath:原路径 targetPath:新路径
	if rootNodeHash == "" {
		return NotSetARootNodeYet()
	}

	if IsFileUsing(originPath) {
		return FileIsUsing()
	}
	originFileName := GetPathFileName(originPath)
	targetFileName := GetPathFileName(targetPath)
	nodeRWLock.Lock()
	originTempPath, err := GetTempPath(GetPathFolder(originPath))
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	originNodeData, err := DecodeNode(originTempPath[len(originTempPath)-1][1], false)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	if _, ok := originNodeData[originFileName]; !ok {
		nodeRWLock.Unlock()
		return NodeDoesNotExist()
	}

	targetTempPath, err := GetTempPath(GetPathFolder(targetPath))
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	targetNodeData, err := DecodeNode(targetTempPath[len(targetTempPath)-1][1], false)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	if _, ok := targetNodeData[targetFileName]; ok {
		nodeRWLock.Unlock()
		return NameExisted()
	}
	delete(nodeCache, originTempPath[len(originTempPath)-1][1])
	delete(nodeCache, targetTempPath[len(targetTempPath)-1][1])
	targetNodeData[targetFileName] = originNodeData[originFileName]

	lastNodeHash, err := CreateNode(targetNodeData, true)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	targetTempPath[len(targetTempPath)-1] = []string{targetTempPath[len(targetTempPath)-1][0], lastNodeHash}
	for i := len(targetTempPath) - 2; i >= 0; i-- {
		targetNodeData, err = DecodeNode(targetTempPath[i][1], false)
		delete(nodeCache, targetTempPath[i][1])
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		targetNodeData[targetTempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(targetNodeData, true)
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		targetTempPath[i] = []string{targetTempPath[i][0], lastNodeHash}
	}
	rootNodeHash = targetTempPath[0][1]

	originTempPath, err = GetTempPath(GetPathFolder(originPath))
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	originNodeData, err = DecodeNode(originTempPath[len(originTempPath)-1][1], false)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	delete(originNodeData, originFileName)

	lastNodeHash, err = CreateNode(originNodeData, true)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	originTempPath[len(originTempPath)-1] = []string{originTempPath[len(originTempPath)-1][0], lastNodeHash}
	for i := len(originTempPath) - 2; i >= 0; i-- {
		originNodeData, err = DecodeNode(originTempPath[i][1], false)
		delete(nodeCache, originTempPath[i][1])
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		originNodeData[originTempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(originNodeData, true)
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		originTempPath[i] = []string{originTempPath[i][0], lastNodeHash}
	}
	rootNodeHash = originTempPath[0][1]
	UploadNode()
	nodeRWLock.Unlock()
	return nil
}

func CopyNode(originPath string, targetPath string) error { //复制一个文件或文件夹(cp) originPath:原路径 targetPath:新路径
	if rootNodeHash == "" {
		return NotSetARootNodeYet()
	}

	if IsFileUsing(originPath) {
		return FileIsUsing()
	}
	originFileName := GetPathFileName(originPath)
	targetFileName := GetPathFileName(targetPath)
	nodeRWLock.Lock()
	originTempPath, err := GetTempPath(GetPathFolder(originPath))
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	originNodeData, err := DecodeNode(originTempPath[len(originTempPath)-1][1], false)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	if _, ok := originNodeData[originFileName]; !ok {
		nodeRWLock.Unlock()
		return NodeDoesNotExist()
	}

	targetTempPath, err := GetTempPath(GetPathFolder(targetPath))
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	targetNodeData, err := DecodeNode(targetTempPath[len(targetTempPath)-1][1], false)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	if _, ok := targetNodeData[targetFileName]; ok {
		nodeRWLock.Unlock()
		return NameExisted()
	}
	delete(nodeCache, targetTempPath[len(targetTempPath)-1][1])
	targetNodeData[targetFileName] = originNodeData[originFileName]

	lastNodeHash, err := CreateNode(targetNodeData, true)
	if err != nil {
		nodeRWLock.Unlock()
		return err
	}
	targetTempPath[len(targetTempPath)-1] = []string{targetTempPath[len(targetTempPath)-1][0], lastNodeHash}
	for i := len(targetTempPath) - 2; i >= 0; i-- {
		targetNodeData, err = DecodeNode(targetTempPath[i][1], false)
		delete(nodeCache, targetTempPath[i][1])
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		targetNodeData[targetTempPath[i+1][0]] = []string{"0", lastNodeHash}
		lastNodeHash, err = CreateNode(targetNodeData, true)
		if err != nil {
			nodeRWLock.Unlock()
			return err
		}
		targetTempPath[i] = []string{targetTempPath[i][0], lastNodeHash}
	}
	rootNodeHash = targetTempPath[0][1]
	UploadNode()
	nodeRWLock.Unlock()
	return nil
}

func GetFileLength(path string) (int64, error) { //获取文件大小 path:文件路径
	if rootNodeHash == "" {
		return 0, NotSetARootNodeYet()
	}

	name := GetPathFileName(path)
	nodeRWLock.Lock()
	tempPath, err := GetTempPath(GetPathFolder(path))
	if err != nil {
		nodeRWLock.Unlock()
		return 0, err
	}
	nodeData, err := DecodeNode(tempPath[len(tempPath)-1][1], true)
	nodeRWLock.Unlock()
	if err != nil {
		return 0, err
	}
	if _, ok := nodeData[name]; !ok {
		return 0, NodeDoesNotExist()
	}
	if nodeData[name][0] == "1" {
		return strconv.ParseInt(nodeData[name][2], 10, 64)
	}
	return 0, NotAFile()
}
