package parser

import (
	"github.com/hatlonely/go-kit/logger"
	"strconv"
	"strings"
)

type InterfaceParser interface {
	Parse(string) ([]string, []string, error)
	Close()
}

type Parser struct {
	fileType string
	runLog   *logger.Logger
}

func NewParser(
	fileType string,
	runLog *logger.Logger,
) *Parser {
	return &Parser{
		fileType: fileType,
		runLog:   runLog,
	}
}

func (p *Parser) Parse(line string) (keyArray []string, buffArray []string, err error) {
	if p.fileType == "tuvssh" {
		// 经度下标，纬度下标，时间戳，海水高度，海水温度
		dataArray := strings.Split(line, ",")
		ts, err := strconv.ParseInt(dataArray[2], 10, 64)
		if err != nil {
			return keyArray, buffArray, err
		}
		tsStr := strconv.FormatInt(ts*1000, 64)
		keyArray = append(keyArray, dataArray[1])
		buffArray = append(buffArray, tsStr, dataArray[3], dataArray[4])
	}
	return keyArray, buffArray, err
}

func (p *Parser) Close() {

}
