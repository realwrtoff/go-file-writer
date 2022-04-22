package parser

import (
	"github.com/hatlonely/go-kit/logger"
	"strings"
)

type InterfaceParser interface {
	Parse(line string) (key string, array []string, err error)
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

func (p *Parser) Parse(line string) (key string, array []string, err error) {
	if p.fileType == "tuvssh" {
		array = strings.Split(line, "\t")
		key = array[1]
	}
	return
}
