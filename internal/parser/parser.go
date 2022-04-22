package parser

import (
	"github.com/hatlonely/go-kit/logger"
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
		buffArray = strings.Split(line, "\t")
		keyArray = append(keyArray, buffArray[0], buffArray[1], buffArray[2])
	}
	return
}

func (p *Parser) Close() {

}