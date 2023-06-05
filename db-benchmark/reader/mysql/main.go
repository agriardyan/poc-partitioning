package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type rawOverallMetric struct {
	durations     []float64
	analyzeResult []float64
	mlock         *sync.Mutex
}

type roundtripResultMetric struct {
	avg               float64
	p50               float64
	p90               float64
	p99               float64
	avgExplainAnalyze float64
	p50ExplainAnalyze float64
	p90ExplainAnalyze float64
	p99ExplainAnalyze float64
}

type analyzeParam struct {
	connStr       string
	routineCount  int
	samplingCount int
	maxOpenConn   int
	maxIdleConn   int
	tablename     string
	withExec      bool
}

type UserData struct {
	UserID          int     `db:"user_id"`
	AccNo           string  `db:"accno"`
	UserSidComplete string  `db:"user_sid_complete"`
	PortoDate       string  `db:"porto_date"`
	PortoStockCode  string  `db:"porto_stock_code"`
	PortoStockQty   float64 `db:"porto_stock_quantity"`
	PortoLastPrice  float64 `db:"porto_last_price"`
	PortoAvgPrice   float64 `db:"porto_avg_price"`
	PortoAmount     float64 `db:"porto_amount"`
	HasCredit       bool    `db:"has_credit"`
}

var (
	pattern = `[0-9]+(?:\.[0-9]+)?`

	// Compile the regular expression
	re = regexp.MustCompile(pattern)
)

func main() {
	var (
		connStr       string
		connStrFile   string
		routineCount  int
		samplingCount int
		maxOpenConn   int
		maxIdleConn   int
		tablename     string
		withExec      bool
		testParamFile string
		sleepSecond   int
	)

	flag.StringVar(&connStr, "connstr", "", "Connection string")
	flag.StringVar(&connStrFile, "connstr-file", "", "Connection string file - will overrides connstr")
	flag.IntVar(&routineCount, "routine-count", 2, "Routine count")
	flag.IntVar(&samplingCount, "sampling-count", 5, "Sampling count")
	flag.IntVar(&maxOpenConn, "max-open-conn", 100, "Max open connection")
	flag.IntVar(&maxIdleConn, "max-idle-conn", 10, "Max idle connection")
	flag.StringVar(&tablename, "tablename", "high_load_prototyping", "Table name")
	flag.BoolVar(&withExec, "with-exec", false, "With exec")
	flag.StringVar(&testParamFile, "test-param-file", "test_param.csv", "Test param file - will overrides all other params")
	flag.IntVar(&sleepSecond, "sleep-second", 0, "Sleep second")

	flag.Parse()

	fmt.Printf("connstr: %s\n", connStr)
	fmt.Printf("connstr file: %s\n", connStrFile)
	fmt.Printf("routine count: %d\n", routineCount)
	fmt.Printf("sampling count: %d\n", samplingCount)
	fmt.Printf("max open conn: %d\n", maxOpenConn)
	fmt.Printf("max idle conn: %d\n", maxIdleConn)
	fmt.Printf("table name: %s\n", tablename)
	fmt.Printf("with exec: %t\n", withExec)
	fmt.Printf("test param file: %s\n", testParamFile)
	fmt.Printf("sleep second: %d\n", sleepSecond)

	content, err := ioutil.ReadFile(connStrFile)
	if err != nil {
		panic(err)
	}

	text := string(content)
	if text != "" {
		connStr = text
	}

	if testParamFile == "" {
		runEach(
			analyzeParam{
				connStr:       connStr,
				routineCount:  routineCount,
				samplingCount: samplingCount,
				maxOpenConn:   maxOpenConn,
				maxIdleConn:   maxIdleConn,
				tablename:     tablename,
				withExec:      withExec,
			})
		return
	}

	f, err := os.Open(testParamFile)
	if err != nil {
		panic(err)
	}

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		panic(err)
	}
	f.Close()

	records = records[1:]

	var analyzeParams []analyzeParam
	for _, rec := range records {
		routineCount, _ := strconv.Atoi(rec[0])
		samplingCount, _ := strconv.Atoi(rec[1])
		maxOpenConn, _ := strconv.Atoi(rec[2])
		maxIdleConn, _ := strconv.Atoi(rec[3])
		tablename := rec[4]
		withExec, _ := strconv.ParseBool(rec[5])

		analyzeParams = append(analyzeParams, analyzeParam{
			connStr:       connStr,
			routineCount:  routineCount,
			samplingCount: samplingCount,
			maxOpenConn:   maxOpenConn,
			maxIdleConn:   maxIdleConn,
			tablename:     tablename,
			withExec:      withExec,
		})
	}

	for i, ap := range analyzeParams {
		startBenchmark := time.Now()
		res := runEach(ap)
		endBenchmark := time.Now()

		fmt.Printf("Test %d\n", i+1)

		fmt.Printf("avg: %f\n", res.avg)
		fmt.Printf("p50: %f\n", res.p50)
		fmt.Printf("p90: %f\n", res.p90)
		fmt.Printf("p99: %f\n", res.p99)

		fmt.Println()

		appendResultCSV([]string{
			startBenchmark.Format("2006-01-02 15:04:05"),
			endBenchmark.Format("2006-01-02 15:04:05"),
			fmt.Sprintf("%d", ap.routineCount),
			fmt.Sprintf("%d", ap.samplingCount),
			fmt.Sprintf("%d", ap.maxOpenConn),
			fmt.Sprintf("%d", ap.maxIdleConn),
			ap.tablename,
			fmt.Sprintf("%t", ap.withExec),
			fmt.Sprintf("%f", res.avg),
			fmt.Sprintf("%f", res.p50),
			fmt.Sprintf("%f", res.p90),
			fmt.Sprintf("%f", res.p99),
			fmt.Sprintf("%f", res.avgExplainAnalyze),
			fmt.Sprintf("%f", res.p50ExplainAnalyze),
			fmt.Sprintf("%f", res.p90ExplainAnalyze),
			fmt.Sprintf("%f", res.p99ExplainAnalyze),
		})

		time.Sleep(time.Duration(sleepSecond) * time.Second)
	}
}

func appendResultCSV(data []string) {
	// Open the CSV file in append mode
	file, err := os.OpenFile("analyze_result.csv", os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)

	// Write the data to the CSV file
	err = writer.Write(data)
	if err != nil {
		panic(err)
	}

	// Flush any buffered data to the file
	writer.Flush()

	// Check for any errors during flushing
	if err := writer.Error(); err != nil {
		panic(err)
	}

	fmt.Println("Data appended to CSV file successfully")
}

func runEach(opt analyzeParam) roundtripResultMetric {
	connStr := opt.connStr
	routineCount := opt.routineCount
	samplingCount := opt.samplingCount
	maxOpenConn := opt.maxOpenConn
	maxIdleConn := opt.maxIdleConn
	tablename := opt.tablename
	withExec := opt.withExec

	fmt.Printf("connstr: %s\n", connStr)
	fmt.Printf("routine count: %d\n", routineCount)
	fmt.Printf("sampling count: %d\n", samplingCount)
	fmt.Printf("max open conn: %d\n", maxOpenConn)
	fmt.Printf("max idle conn: %d\n", maxIdleConn)
	fmt.Printf("table name: %s\n", tablename)
	fmt.Printf("with exec: %t\n", withExec)

	met := &rawOverallMetric{
		mlock: &sync.Mutex{},
	}

	fmt.Printf("connstr: %s\n", connStr)
	fmt.Printf("routine count: %d\n", routineCount)
	fmt.Printf("sampling count: %d\n", samplingCount)
	fmt.Printf("max open conn: %d\n", maxOpenConn)
	fmt.Printf("max idle conn: %d\n", maxIdleConn)
	fmt.Printf("table name: %s\n", tablename)

	fmt.Println()

	db := sqlx.MustConnect("mysql", connStr)
	defer db.Close()
	db.DB.SetMaxOpenConns(maxOpenConn)
	db.DB.SetMaxIdleConns(maxIdleConn)

	stocks := []string{"GOTO", "BBCA", "BBRI", "ADRO", "ANTM", "SIDO", "BUMI", "BMRI", "TLKM", "BRIS"}

	startSig := make(chan struct{})
	stopCh := make(chan struct{})

	wg := &sync.WaitGroup{}
	for i := 0; i < routineCount; i++ {
		wg.Add(1)
		go func(start <-chan struct{}) {
			<-start

			fmt.Println("starting worker")

			for _, stock := range stocks {
				execDurs, analyzeRes := func(stockCode string, iteration int) (execDuration []float64, result []float64) {
					querySelect := fmt.Sprintf(`select * from `+tablename+` where porto_stock_code = '%s' AND has_credit = true`, stockCode)

					for i := 0; i < iteration; i++ {
						start := time.Now()
						if withExec {
							runExec(db, querySelect)
						} else {
							runSelect(db, querySelect)
						}
						milis := time.Since(start).Microseconds()
						execDuration = append(execDuration, float64(milis)/1000)
					}

					return execDuration, result
				}(stock, samplingCount)

				sort.Float64s(execDurs)

				fmt.Println(stock)

				fmt.Printf("avg e2e durs: %.5f\n", calculateAverage(execDurs))
				fmt.Printf("p50 e2e durs: %.5f\n", calculatePercentile(execDurs, 50))
				fmt.Printf("p90 e2e durs: %.5f\n", calculatePercentile(execDurs, 90))
				fmt.Printf("p99 e2e durs: %.5f\n", calculatePercentile(execDurs, 99))

				sort.Float64s(analyzeRes)

				fmt.Printf("avg explain analyze: %.5f\n", calculateAverage(analyzeRes))
				fmt.Printf("p50 explain analyze: %.5f\n", calculatePercentile(analyzeRes, 50))
				fmt.Printf("p90 explain analyze: %.5f\n", calculatePercentile(analyzeRes, 90))
				fmt.Printf("p99 explain analyze: %.5f\n", calculatePercentile(analyzeRes, 99))
				fmt.Println()

				met.mlock.Lock()
				met.durations = append(met.durations, execDurs...)
				met.analyzeResult = append(met.analyzeResult, analyzeRes...)
				met.mlock.Unlock()
			}
			wg.Done()
		}(startSig)
	}

	time.Sleep(1 * time.Second)
	close(startSig)

	wg.Wait()
	close(stopCh)

	overallExecDurs := met.durations
	overallAnalyzeDurs := met.analyzeResult

	sort.Float64s(overallExecDurs)
	sort.Float64s(overallAnalyzeDurs)

	avg := calculateAverage(overallExecDurs)
	p50 := calculatePercentile(overallExecDurs, 50)
	p90 := calculatePercentile(overallExecDurs, 90)
	p99 := calculatePercentile(overallExecDurs, 99)

	avgExplainAnalyze := calculateAverage(overallAnalyzeDurs)
	p50ExplainAnalyze := calculatePercentile(overallAnalyzeDurs, 50)
	p90ExplainAnalyze := calculatePercentile(overallAnalyzeDurs, 90)
	p99ExplainAnalyze := calculatePercentile(overallAnalyzeDurs, 99)

	fmt.Println()

	return roundtripResultMetric{
		avg:               avg,
		p50:               p50,
		p90:               p90,
		p99:               p99,
		avgExplainAnalyze: avgExplainAnalyze,
		p50ExplainAnalyze: p50ExplainAnalyze,
		p90ExplainAnalyze: p90ExplainAnalyze,
		p99ExplainAnalyze: p99ExplainAnalyze,
	}

}

func runExec(db *sqlx.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		panic(err)
	}
}

func runSelect(db *sqlx.DB, query string) {
	rows, err := db.Queryx(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		panic(err)
	}

	for rows.Next() {
		var data UserData
		if err := rows.StructScan(&data); err != nil {
			panic(err)
		}
	}

}

func calculateAverage(values []float64) float64 {
	sum := 0.0
	for _, val := range values {
		sum += val
	}
	return sum / float64(len(values))
}

func calculatePercentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}

	index := int(math.Ceil(float64(len(values)) * percentile / 100.0))
	if index == 0 {
		return values[0]
	} else if index >= len(values) {
		return values[len(values)-1]
	}

	return values[index-1]
}
