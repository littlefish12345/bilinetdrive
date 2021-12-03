package bilinetdrive

import (
	"path/filepath"
	"strings"
)

func PathSwitchDir(orginalPath string, relPath string) (string, error) { //绝对路径转相对路径 orginalPath:原有绝对路径
	pathList := strings.Split(relPath, "/")
	nowPathList := strings.Split(orginalPath, "/")
	nowPathList = nowPathList[1:]
	for i := 0; i < len(pathList); i++ {
		if pathList[i] == "." {
			continue
		} else if pathList[i] == "" && i == 0 {
			nowPathList = []string{nowPathList[0]}
			if len(pathList) == 2 {
				if pathList[1] == "" {
					break
				}
			}
		} else if pathList[i] == ".." {
			if len(nowPathList) > 0 && nowPathList[0] != "" {
				nowPathList = nowPathList[0 : len(nowPathList)-1]
			} else {
				return "", PathDoesNotExist()
			}
		} else {
			if nowPathList[0] == "" {
				nowPathList[0] = pathList[i]
			} else {
				nowPathList = append(nowPathList, pathList[i])
			}
		}
	}
	return "/" + strings.Join(nowPathList, "/"), nil
}

func JoinPath(first string, second string) string {
	path := filepath.Join(first, second)
	pathList := strings.Split(path, "\\")
	return strings.Join(pathList, "/")
}

func GetPathFileName(path string) string {
	pathList := strings.Split(path, "/")
	return pathList[len(pathList)-1]
}

func GetPathFolder(path string) string {
	pathList := strings.Split(path, "/")
	pathList = pathList[0 : len(pathList)-1]
	if len(pathList) == 1 {
		return "/"
	}
	return strings.Join(pathList, "/")
}
