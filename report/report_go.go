//$GOPATH/bin/go run $0 $@ ; exit
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"text/template"
)

type types string

const (
	hero   types = "hero"
	kol    types = "kol"
	seller types = "seller"
)

const (
	address       = "broadcaster-report-grpc.gke-infra-production-asia-southeast1-main-production.k8s:50051"
	method        = "broadcaster_report.Report/SummariesBulk"
	defHeroPath   = "hero"
	defKolPath    = "kol"
	defSellerPath = "seller"
	reqFormat     = "{\"callerContext\": {\"tribe\": \"Content\",\"squad\": \"Script\"},\"channelIDs\": [%s],\"userID\": 32077238}"
	printFormat   = `Summary Report - %s - %s
Add To Cart         : %s
Remove From Cart    : %s
Wishlist            : %s
Remove Wishlist     : %s
Payment Verified    : %s
Follow Shop         : %s
Unfollow Shop       : %s
Like Channel        : %s
Unlike Channel      : %s
Visit Shop          : %s
Visit PDP           : %s
Visit Channel       : %s

`
)

var args struct {
	path  map[types]string
	types []types
}

var (
	channel_type string
	hero_path    string
	kol_path     string
	seller_path  string
)

type response struct {
	ReportData map[uint64]struct {
		Channel struct {
			Metrics map[string]string `json:"metrics"`
		} `json:"Channel"`
	} `json:"reportData"`
}

type htmlPayload struct {
	Title   string
	Metrics map[string]string
}

func init() {
	flag.StringVar(&hero_path, string(hero), defHeroPath, "file path for hero channels")
	flag.StringVar(&kol_path, string(kol), defKolPath, "file path for kol channels")
	flag.StringVar(&seller_path, string(seller), defSellerPath, "file path for seller channels")
	flag.StringVar(&channel_type, "channel_type", "all", "channel type to process (hero|kol|seller|all)")
}

func main() {
	flag.Parse()
	parseArgs()

	tmpl := template.Must(template.ParseFiles("index.html"))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		payloads := []htmlPayload{}
		for _, channelType := range args.types {
			payload := process(args.path[channelType], channelType)
			payloads = append(payloads, payload...)
		}

		tmpl.Execute(w, payloads)
	})
	http.ListenAndServe(":8000", nil)
}

func process(path string, channelType types) []htmlPayload {
	log.Printf("processing %s channels", channelType)

	channelIDs, err := readFile(path)
	if err != nil {
		log.Printf("error reading file %v", err)
		return []htmlPayload{}
	}

	htmlPayloads := []htmlPayload{}
	for _, channelID := range channelIDs {
		grpcURLPath, err := exec.LookPath("grpcurl")
		if err != nil {
			log.Println("grpcurl not found")
			return []htmlPayload{}
		}

		var rawResponse bytes.Buffer
		grpcURLExec := &exec.Cmd{
			Path: grpcURLPath,
			Args: []string{grpcURLPath,
				"-format",
				"json",
				"-plaintext",
				"-d",
				fmt.Sprintf(reqFormat, channelID),
				address,
				method},
			Stdout: &rawResponse,
			Stderr: os.Stdout,
		}

		if err := grpcURLExec.Run(); err != nil {
			log.Println("error executing grpcurl")
			return []htmlPayload{}
		}

		var res response
		err = json.Unmarshal(rawResponse.Bytes(), &res)
		if err != nil {
			log.Println("error parsing result")
			log.Println(rawResponse.String())
			log.Printf("err: %v\n", err)
			return []htmlPayload{}
		}

		var result = make(map[string]string)
		for _, data := range res.ReportData {
			for key, metric := range data.Channel.Metrics {
				val := metric
				if _, ok := result[key]; !ok {
					result[key] = "0"
				}

				a, err := strconv.ParseInt(result[key], 10, 64)
				if err != nil {
					log.Println("error parsing result")
					log.Println(result[key])
					log.Printf("err: %v\n", err)
					return []htmlPayload{}
				}
				b, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					log.Println("error parsing result")
					log.Println(val)
					log.Printf("err: %v\n", err)
					return []htmlPayload{}
				}
				result[key] = strconv.FormatInt(a+b, 10)
			}
		}

		p := htmlPayload{
			// Title:   fmt.Sprintf("Summary Report - %s - %s", string(channelType), time.Now().Format(time.UnixDate)),
			Title: fmt.Sprintf("%s - %s", string(channelType), channelID),
			// Title:   string(channelType),
			Metrics: result,
		}

		htmlPayloads = append(htmlPayloads, p)
	}

	return htmlPayloads
}

func readFile(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines, scanner.Err()
}

func parseArgs() {
	switch channel_type {
	case "hero":
		args.types = append(args.types, hero)
	case "kol":
		args.types = append(args.types, kol)
	case "seller":
		args.types = append(args.types, seller)
	case "all":
		args.types = append(args.types, hero, kol, seller)
	default:
		log.Printf("unrecognized types: %s\n", channel_type)
		os.Exit(1)
	}

	args.path = map[types]string{
		hero:   hero_path,
		kol:    kol_path,
		seller: seller_path,
	}
}
