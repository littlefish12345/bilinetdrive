package bilinetdrive

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io/ioutil"
	"math"
	"mime/multipart"
	"net/http"
	"runtime"
	"strconv"
	"strings"
)

//除非有信心不然不要用这里的函数！

func DecodeImage(imageData *bytes.Buffer) (*bytes.Buffer, error) { //解码图像
	fileImage, err := png.Decode(imageData)
	if err != nil {
		runtime.GC()
		return nil, err
	}
	dataBuffer := new(bytes.Buffer)
	pointColor := color.NRGBAModel.Convert(fileImage.At(fileImage.Bounds().Min.X, fileImage.Bounds().Min.Y)).(color.NRGBA)
	colorBuffer := []byte{pointColor.R, pointColor.G, pointColor.B, pointColor.A}
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

func EncodeImage(data *bytes.Buffer, imageWidth int, imageMaxHeight int) (*bytes.Buffer, error) { //编码图像
	if imageWidth < 10 {
		imageWidth = 10
	}
	if imageMaxHeight < 10 {
		imageMaxHeight = 10
	}
	var singleImageMaxSize int64 = (int64(imageWidth)*int64(imageMaxHeight) - 1) * 4

	buffer := data.Bytes()
	num := int64(data.Len())
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
	if err != nil {
		runtime.GC()
		return nil, err
	}
	response.Body.Close()
	runtime.GC()
	return bytes.NewBuffer(imageData), nil
}

func PushImage(imageData *bytes.Buffer) (string, error) { //上传图片
	body := new(bytes.Buffer)
	postData := multipart.NewWriter(body)
	part1, err := postData.CreateFormFile("file_up", "file.png")
	if err != nil {
		runtime.GC()
		return "", err
	}
	_, err = part1.Write(imageData.Bytes())
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
			return "", errors.New("upload Failure: " + strconv.FormatInt(int64(v), 10) + " " + jsonData["message"].(string))
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
	return "", errors.New("upload Failure: error json format")
}
