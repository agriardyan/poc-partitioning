package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/alitto/pond"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var portoDate = time.Date(2023, 5, 31, 0, 0, 0, 0, time.UTC)

type LatencyMetric struct {
	cummulativeLatency float64
	totalOp            int
	maxLatency         float64
	lock               *sync.Mutex
}

var latencyMetric *LatencyMetric

type analyzeParam struct {
	connStr       string
	accNoPath     string
	stockDataPath string
	routineCount  int
	samplingCount int
	dummyAmount   int
	maxOpenConn   int
	maxIdleConn   int
	tablename     string
	batchRow      int
}

type UserData struct {
	UserID          int       `db:"user_id"`
	AccNo           string    `db:"accno"`
	UserSidComplete string    `db:"user_sid_complete"`
	PortoDate       time.Time `db:"porto_date"`
	PortoStockCode  string    `db:"porto_stock_code"`
	PortoStockQty   float64   `db:"porto_stock_quantity"`
	PortoLastPrice  float64   `db:"porto_last_price"`
	PortoAvgPrice   float64   `db:"porto_avg_price"`
	PortoAmount     float64   `db:"porto_amount"`
	HasCredit       bool      `db:"has_credit"`
}

func loadAccNos(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Read the file line by line and store the lines in an array
	lines := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return lines
}

func portfolioFromCsv(stockDataPath string) []portfolioGenData {
	filePath := stockDataPath

	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all the records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	var portfolios []portfolioGenData
	for _, rec := range records {
		stockCode := rec[0]
		startIdx, _ := strconv.Atoi(rec[2])
		endIdx, _ := strconv.Atoi(rec[3])
		lastPrice := genRandomFloat(3000, 5000)
		genAvgPriceMin := genRandomFloat(3000, 7000)
		genAvgPriceMax := genRandomFloat(1000, 3000)
		portfolios = append(portfolios, portfolioGenData{
			stockCode:      stockCode,
			startIdx:       startIdx,
			endIdx:         endIdx,
			lastPrice:      lastPrice,
			genAvgPriceMin: genAvgPriceMin,
			genAvgPriceMax: genAvgPriceMax,
		})
	}

	return portfolios
}

type portfolioGenData struct {
	stockCode      string
	startIdx       int
	endIdx         int
	lastPrice      float64
	genAvgPriceMin float64
	genAvgPriceMax float64
}

func main() {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		panic(err)
	}

	var (
		connStr       string
		connStrFile   string
		routineCount  int
		samplingCount int
		dummyAmount   int
		maxOpenConn   int
		maxIdleConn   int
		accNoPath     string
		stockDataPath string
		tablename     string
		testParamFile string
		sleepSecond   int
		batchRow      int
	)

	flag.StringVar(&connStr, "connstr", "user=admin password=admin dbname=postgres sslmode=disable", "postgres connection string")
	flag.StringVar(&connStrFile, "connstr-file", "", "Connection string file - will overrides connstr")
	flag.IntVar(&routineCount, "maxworker", 10, "max worker")
	flag.IntVar(&samplingCount, "maxworkercap", 1000, "max worker cap")
	flag.IntVar(&dummyAmount, "dummy-amount", 1800000, "remaining dummy amount")
	flag.IntVar(&maxOpenConn, "maxconn", 100, "max connection")
	flag.IntVar(&maxIdleConn, "maxidleconn", 10, "max idle connection")
	flag.StringVar(&accNoPath, "accnopath", "./accnos_500k.txt", "path to accnos file")
	flag.StringVar(&stockDataPath, "stockdatapath", "./stock_data.csv", "path to stock data csv")
	flag.StringVar(&tablename, "tablename", "high_load_prototyping", "table name")
	flag.StringVar(&testParamFile, "test-param-file", "test_param.csv", "Test param file - will overrides all other params")
	flag.IntVar(&sleepSecond, "sleep-second", 5, "Sleep second")
	flag.IntVar(&batchRow, "batch-row", 1, "Batch row")

	flag.Parse()

	fmt.Printf("connstr: %s\n", connStr)
	fmt.Printf("connstr file: %s\n", connStrFile)
	fmt.Printf("routine count: %d\n", routineCount)
	fmt.Printf("sampling count: %d\n", samplingCount)
	fmt.Printf("dummy amount: %d\n", dummyAmount)
	fmt.Printf("max open conn: %d\n", maxOpenConn)
	fmt.Printf("max idle conn: %d\n", maxIdleConn)
	fmt.Printf("accno path: %s\n", accNoPath)
	fmt.Printf("stock data path: %s\n", stockDataPath)
	fmt.Printf("table name: %s\n", tablename)
	fmt.Printf("test param file: %s\n", testParamFile)
	fmt.Printf("sleep second: %d\n", sleepSecond)
	fmt.Printf("batch row: %d\n", batchRow)
	fmt.Println()

	ctx := context.Background()

	content, err := ioutil.ReadFile(connStrFile)
	if err != nil {
		panic(err)
	}

	text := string(content)
	if text != "" {
		connStr = text
	}

	latencyMetric = &LatencyMetric{
		lock: &sync.Mutex{},
	}

	if testParamFile == "" {
		runEach(
			ctx,
			analyzeParam{
				connStr:       connStr,
				routineCount:  routineCount,
				samplingCount: samplingCount,
				dummyAmount:   dummyAmount,
				maxOpenConn:   maxOpenConn,
				maxIdleConn:   maxIdleConn,
				accNoPath:     accNoPath,
				stockDataPath: stockDataPath,
				tablename:     tablename,
				batchRow:      batchRow,
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

		analyzeParams = append(analyzeParams, analyzeParam{
			connStr:       connStr,
			routineCount:  routineCount,
			samplingCount: samplingCount,
			dummyAmount:   dummyAmount,
			maxOpenConn:   maxOpenConn,
			maxIdleConn:   maxIdleConn,
			tablename:     tablename,
			accNoPath:     accNoPath,
			stockDataPath: stockDataPath,
			batchRow:      batchRow,
		})
	}

	for i, ap := range analyzeParams {
		startBenchmark := time.Now().In(loc)
		runEach(ctx, ap)
		endBenchmark := time.Now().In(loc)
		timeSpent := time.Since(startBenchmark)

		fmt.Printf("Test %d\n", i+1)

		fmt.Println()

		latencyMetric.lock.Lock()
		cummLatency := latencyMetric.cummulativeLatency
		maxLatency := latencyMetric.maxLatency

		avgLatency := cummLatency / float64(latencyMetric.totalOp)

		appendResultCSV([]string{
			startBenchmark.Format("2006-01-02 15:04:05"),
			endBenchmark.Format("2006-01-02 15:04:05"),
			timeSpent.String(),
			fmt.Sprintf("%.2f", avgLatency),
			fmt.Sprintf("%.2f", maxLatency),
			fmt.Sprintf("%d", ap.routineCount),
			fmt.Sprintf("%d", ap.samplingCount),
			fmt.Sprintf("%d", ap.maxOpenConn),
			fmt.Sprintf("%d", ap.maxIdleConn),
			fmt.Sprintf("%d", ap.batchRow),
			ap.tablename,
		})

		latencyMetric.totalOp = 0
		latencyMetric.cummulativeLatency = 0
		latencyMetric.maxLatency = 0

		latencyMetric.lock.Unlock()

		time.Sleep(time.Duration(sleepSecond) * time.Second)
	}

}

func runEach(ctx context.Context, opt analyzeParam) {
	connStr := opt.connStr
	tablename := opt.tablename
	routineCount := opt.routineCount
	samplingCount := opt.samplingCount
	accNoPath := opt.accNoPath
	stockDataPath := opt.stockDataPath
	remainingDummyAmount := opt.dummyAmount
	maxOpenConn := opt.maxOpenConn
	maxIdleConn := opt.maxIdleConn
	batchRow := opt.batchRow

	start := time.Now()

	accNos := loadAccNos(accNoPath)
	portfolios := portfolioFromCsv(stockDataPath)

	db := sqlx.MustConnect("mysql", connStr)
	defer func() {
		db.Close()
	}()
	db.SetMaxOpenConns(maxOpenConn)
	db.SetMaxIdleConns(maxIdleConn)

	err := truncateTable(ctx, db, tablename)
	if err != nil {
		panic(err)
	}

	worker := pond.New(routineCount, samplingCount)

	fmt.Println("called distribute & genrandom")

	distribute(ctx, worker, db, tablename, portfolios, accNos, batchRow)
	genRandomDummyData(ctx, worker, db, tablename, accNos[400000:], remainingDummyAmount, batchRow)

	worker.StopAndWait()

	duration := time.Since(start)
	fmt.Println("generate all data done at: ", time.Now())
	fmt.Println("generate all data duration: ", duration)

}

func genRandomDummyData(ctx context.Context, worker *pond.WorkerPool, db *sqlx.DB, table string, accNos []string, amount int, batchRow int) {
	fmt.Println("generating remaining random data...")

	fmt.Println("genrandom dummy params")
	fmt.Printf("amount: %d\n", amount)
	fmt.Printf("accno count: %d\n", len(accNos))

	accLen := len(accNos)
	numIter := (amount / batchRow) + (amount % batchRow)
	for i := 0; i < numIter; i++ {
		accNo := accNos[i%accLen]
		uid, err := strconv.Atoi(accNo[10:])
		if err != nil {
			panic(err)
		}

		var userDatas []UserData
		for i := 0; i < batchRow; i++ {
			userData := UserData{
				UserID:          uid,
				AccNo:           accNo,
				UserSidComplete: accNo,
				PortoDate:       portoDate,
				PortoStockCode:  fmt.Sprintf("OTH%d", genRandomInt(200, 500)),
				PortoStockQty:   genRandomFloat(1000000, 2000000),
				PortoLastPrice:  genRandomFloat(1000, 5000),
				PortoAvgPrice:   genRandomFloat(1000, 5000),
				PortoAmount:     genRandomFloat(1000, 5000),
				HasCredit:       gen2PercentBoolTrue(),
			}

			userDatas = append(userDatas, userData)
		}

		worker.Submit(func() {
			start := time.Now()
			err := seedTable(ctx, db, table, userDatas)
			end := time.Since(start)

			latencyMetric.lock.Lock()
			latencyMilis := float64(end.Microseconds()) / 1000

			latencyMetric.totalOp++
			latencyMetric.cummulativeLatency += latencyMilis
			if latencyMetric.maxLatency < latencyMilis {
				latencyMetric.maxLatency = latencyMilis
			}

			fmt.Printf("genrandom latency: %.2f\n", latencyMilis)

			latencyMetric.lock.Unlock()

			if err != nil {
				panic(err)
			}
		})
	}

	fmt.Println("done submit seed remaining dummy data")
}

func distribute(ctx context.Context, worker *pond.WorkerPool, db *sqlx.DB, table string, pfolios []portfolioGenData, accNos []string, batchRow int) {
	fmt.Println("generating actual stock ...")

	fmt.Println("distribute params")
	fmt.Println("len pfolios", len(pfolios))
	fmt.Println("len accNos", len(accNos))

	for _, p := range pfolios {
		currIdx := p.startIdx
		endIdx := currIdx + batchRow

		for {
			lastIter := false
			if endIdx > p.endIdx {
				endIdx = p.endIdx
				lastIter = true
			}

			var userDatas []UserData
			for _, accNo := range accNos[currIdx:endIdx] {
				uid, err := strconv.Atoi(accNo[10:])
				if err != nil {
					panic(err)
				}

				userData := UserData{
					UserID:          uid,
					AccNo:           accNo,
					UserSidComplete: accNo,
					PortoDate:       portoDate,
					PortoStockCode:  p.stockCode,
					PortoStockQty:   genRandomFloat(1000000, 2000000),
					PortoLastPrice:  p.lastPrice,
					PortoAvgPrice:   genRandomFloat(p.genAvgPriceMin, p.genAvgPriceMax),
					PortoAmount:     genRandomFloat(p.genAvgPriceMin, p.genAvgPriceMax),
					HasCredit:       gen2PercentBoolTrue(),
				}

				userDatas = append(userDatas, userData)
			}

			worker.Submit(func() {
				start := time.Now()
				err := seedTable(ctx, db, table, userDatas)
				end := time.Since(start)

				latencyMetric.lock.Lock()
				latencyMilis := float64(end.Microseconds()) / 1000

				latencyMetric.totalOp++
				latencyMetric.cummulativeLatency += latencyMilis
				if latencyMetric.maxLatency < latencyMilis {
					latencyMetric.maxLatency = latencyMilis
				}

				fmt.Printf("distribute latency: %.2f\n", latencyMilis)

				latencyMetric.lock.Unlock()
				if err != nil {
					panic(err)
				}
			})

			if lastIter {
				break
			}

			currIdx = currIdx + batchRow
			endIdx = currIdx + batchRow
		}

		fmt.Println("submitted seed stock " + p.stockCode)
	}

	fmt.Println("done submit seed actual stock")
}

func seedTable(ctx context.Context, db *sqlx.DB, table string, users []UserData) error {
	if len(users) == 0 {
		return nil
	}

	query := `INSERT INTO ` + table + ` (user_id, accno, user_sid_complete, porto_date, porto_stock_code, porto_stock_quantity, porto_last_price, porto_avg_price, porto_amount, has_credit) VALUES (:user_id, :accno, :user_sid_complete, :porto_date, :porto_stock_code, :porto_stock_quantity, :porto_last_price, :porto_avg_price, :porto_amount, :has_credit)`

	_, err := db.NamedExecContext(ctx, query, users)
	if err != nil {
		panic(err)
	}

	return nil
}

func truncateTable(ctx context.Context, db *sqlx.DB, table string) error {
	query := `truncate table ` + table
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		panic(err)
	}

	return nil
}

func appendResultCSV(data []string) {
	// Open the CSV file in append mode
	file, err := os.OpenFile("insert_result.csv", os.O_WRONLY|os.O_APPEND, 0644)
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

func genUniqueStrings(numStrings int) []string {
	uniqueStrings := make([]string, 0)
	uniqueSet := make(map[string]bool)

	counter := 1
	for len(uniqueStrings) < numStrings {
		randomString := genRandomString(10)
		randomString = fmt.Sprintf("%s%d", randomString, counter)

		if !uniqueSet[randomString] {
			uniqueSet[randomString] = true
			uniqueStrings = append(uniqueStrings, randomString)
			counter++
		}
	}

	return uniqueStrings
}

func genRandomString(length int) string {
	numBytes := (length * 6) / 8
	randomBytes := make([]byte, numBytes)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}
	randomString := base64.URLEncoding.EncodeToString(randomBytes)
	randomString = randomString[:length]
	return randomString
}

func genRandomFloat(min, max float64) float64 {
	randomRangeFloat := min + rand.Float64()*(max-min)
	return randomRangeFloat
}

func genRandomInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min+1) + min
}

func gen2PercentBoolTrue() bool {
	rand.Seed(time.Now().UnixNano())

	chance := 2 // Chance of getting true in percentage

	// Generate a random number between 0 and 99
	randomNum := rand.Intn(100)

	// Check if the random number falls within the desired chance range
	isTrue := randomNum < chance

	return isTrue
}
