// Command aggregate-panels crawls a directory containing
// multiple panel files of the same type and combines them
// into a single source representing the tiem span covered
// by all of the individual files.
// This is intended to standardize the data cadence across
// panel/survey vendors; i.e. daily panels vs. weekly panels

package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	panel    = flag.String("panel", "", "name of panel type to aggregate")
	filesDir = flag.String("files", "", "directory of files")
	sep      = flag.String("sep", "\t", "file separator")
	output   = flag.String("output", "", "output file")
)

type measurable struct {
	weights  []float64
	features string
}

func average(vals []float64) float64 {
	var sum float64
	for _, val := range vals {
		sum += val
	}
	return sum / float64(len(vals))
}

func main() {
	flag.Parse()

	files, err := ioutil.ReadDir(*filesDir)
	if err != nil {
		log.Fatal(err)
	}

	// it is assumed date is part of the file names
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	var days int
	var headerRow []string
	panelists := make(map[string]measurable)

	for _, file := range files {
		// skip any erroneous hidden files
		if strings.HasPrefix(file.Name(), ".") {
			continue
		}
		days++
		log.Println("working file: ", file.Name())

		filePath := strings.Join([]string{*filesDir, file.Name()}, "/")
		openFile, _ := os.Open(filePath)
		defer openFile.Close()

		csvReader := csv.NewReader(bufio.NewReader(openFile))
		csvReader.Comma = '\t'
		csvReader.FieldsPerRecord = -1

		// process header
		if header, err := csvReader.Read(); err != nil {
			log.Fatal(err)
		} else if len(headerRow) == 0 {
			headerRow = header
		}

		for {
			line, err := csvReader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}

			// replace ',' in weight if its there
			wgtStr := strings.Replace(line[1], ",", ".", 1)
			wgt, err := strconv.ParseFloat(wgtStr, 64)
			if err != nil {
				log.Fatal(err)
			}

			panelist := line[0]
			feats := strings.Join(line[2:], "\t")
			measured := panelists[panelist]

			// include 0 weights for now - if 0 theyre not in-tab so the weights
			// may be adjusted on other days to reflect this
			measured.weights = append(measured.weights, wgt)
			measured.features = feats

			panelists[panelist] = measured // update panelist values
		}
	}

	outFile, err := os.Create(*output)
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)

	n, err := writer.WriteString(strings.Join(headerRow, "\t") + "\n")
	if err != nil {
		log.Println(n)
		log.Fatal(err)
	}
	for panelist, measurements := range panelists {
		writer.WriteString(panelist + "\t")
		writer.WriteString(strconv.FormatFloat(average(measurements.weights), 'f', 5, 64) + "\t")
		writer.WriteString(measurements.features)
		writer.WriteString("\n")
	}
	writer.Flush()
}
