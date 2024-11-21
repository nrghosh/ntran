package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/rand"
)

type ColdNeonDBClient struct {
	mainConnStr string
}

type BranchInfo struct {
	Name    string
	ConnStr string
}

type ExecutionResult struct {
	BranchName string
	Statement  Statement
	Values     []any
	Error      error
}

func (c *ColdNeonDBClient) GetName() string {
	return "cold-neondb"
}

func (c *ColdNeonDBClient) Scaffold(inFlight int) error {
	c.mainConnStr = c.getConnectionString("main")
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, balance INTEGER);")
	if err != nil {
		return err
	}
	return nil
}

func (c *ColdNeonDBClient) GenerateSQL(inFlight int) ([]TestCase, error) {

	var shortInsertStatements []Statement
	for i := 0; i < inFlight; i++ {
		statement := Statement{
			Command: fmt.Sprintf("INSERT INTO users (id, balance) VALUES (%d, 100)", i+1),
			Query:   "SELECT * FROM users;",
		}
		shortInsertStatements = append(shortInsertStatements, statement)
	}

	testCases := []TestCase{
		{
			Name:       "Short Insert",
			Statements: shortInsertStatements,
		},
	}

	return testCases, nil
}

func (c *ColdNeonDBClient) runNeonCmd(idempotentError string, args ...string) string {
	exponents := []float64{1, 2, 3, 4, 5}
	var err error
	var stdout strings.Builder
	var stderr strings.Builder
	shouldBreak := false
	for {
		for _, exponent := range exponents {
			cmd := exec.Command("neon", args...)
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err = cmd.Run()
			if err == nil || (idempotentError != "" && strings.Contains(err.Error(), idempotentError)) {
				shouldBreak = true
				break
			} else {
				log.Printf("error running command \"%v\" %v", args, err)
				log.Print(stderr.String())
				time.Sleep(time.Duration(math.Pow(2, exponent)) * time.Second)
			}
		}
		if shouldBreak {
			break
		}
	}

	return stdout.String()
}

func (c *ColdNeonDBClient) deleteBranch(name string) {
	c.runNeonCmd(fmt.Sprintf("branch %s not found", name), "branch", "delete", name)
}

func (c *ColdNeonDBClient) getConnectionString(branchName string) string {
	stdout := c.runNeonCmd("", "connection-string", branchName)
	return strings.TrimSpace(stdout)
}

func (c *ColdNeonDBClient) createBranch(name string) string {
	stdout := c.runNeonCmd("branch already exists", "branch", "create", "--name", name, "--output", "json")

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		log.Fatal(err)
	}

	if uris, ok := result["connection_uris"].([]interface{}); ok {
		if len(uris) > 0 {
			if uri, ok := uris[0].(map[string]interface{})["connection_uri"]; ok {
				return uri.(string)
			}
		}
	}

	log.Fatal("unable to completely create a branch; might have left dangling branches")
	return ""
}

func (c *ColdNeonDBClient) commit(statement Statement) error {
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if statement.Command != "" {
		_, err = conn.Exec(context.Background(), statement.Command)
	} else {
		_, err = conn.Query(context.Background(), statement.Query)
	}

	if err != nil {
		return err
	}
	return nil
}

func execute(mainConnStr string, statement Statement, branchInfoMap map[string]BranchInfo, wg *sync.WaitGroup, ch chan ExecutionResult) {
	defer wg.Done()

	var rows pgx.Rows
	var branchName string
	if statement.Command != "" {
		if branchInfo, ok := branchInfoMap[statement.Command]; ok {
			branchName = branchInfo.Name
			conn, err := pgx.Connect(context.Background(), branchInfo.ConnStr)
			if err != nil {
				ch <- ExecutionResult{Error: err}
				return
			}
			defer conn.Close(context.Background())

			_, err = conn.Exec(context.Background(), statement.Command)
			if err != nil {
				ch <- ExecutionResult{Error: err}
				return
			}

			rows, err = conn.Query(context.Background(), statement.Query)
			if err != nil {
				ch <- ExecutionResult{Error: err}
				return
			}
		}
	} else {
		branchName = "main"
		conn, err := pgx.Connect(context.Background(), mainConnStr)
		if err != nil {
			ch <- ExecutionResult{Error: err}
			return
		}
		defer conn.Close(context.Background())

		rows, err = conn.Query(context.Background(), statement.Query)
		if err != nil {
			ch <- ExecutionResult{Error: err}
			return
		}
	}
	if rows.Next() {
		v, err := rows.Values()
		if err != nil {
			ch <- ExecutionResult{Error: err}
			return
		}
		ch <- ExecutionResult{BranchName: branchName, Statement: statement, Values: v}
	}
}

func (c *ColdNeonDBClient) Execute(testCases []TestCase, experiment *Experiment) error {
	rand.Seed(uint64(time.Now().UnixNano()))
	for i, testCase := range testCases {
		benchmark := Benchmark{
			Experiment:       experiment,
			Policy:           c.GetName(),
			TestCase:         testCase.Name,
			TransactionCount: len(testCase.Statements),
		}
		benchmark.Start()

		branchInfoMap := make(map[string]BranchInfo)

		// assume all the commands are different. will need to rework if some end
		// up being the same.

		for j, statement := range testCase.Statements {
			if statement.Command != "" {
				if _, ok := branchInfoMap[statement.Command]; !ok {
					db := fmt.Sprintf("db_%v_%v", i, j)
					branchInfoMap[statement.Command] = BranchInfo{Name: db, ConnStr: c.createBranch(db)}
				}
			}
		}

		var results []ExecutionResult
		ch := make(chan ExecutionResult)
		var wg sync.WaitGroup

		for _, statement := range testCase.Statements {
			wg.Add(1)
			go execute(c.mainConnStr, statement, branchInfoMap, &wg, ch)
		}

		go func() {
			wg.Wait()
			close(ch)
		}()

		for result := range ch {
			if result.Error == nil {
				results = append(results, result)
			}
		}

		// dummy "consensus" step here -- take a random one.
		// should _not_ close db that wins consensus.
		// instead, make that the new mainConnStr (I think).
		idx := rand.Intn(len(results))
		err := c.commit(results[idx].Statement)
		if err != nil {
			log.Default().Println(err)
		}

		benchmark.End()
		benchmark.Log()

		for _, branchInfo := range branchInfoMap {
			c.deleteBranch(branchInfo.Name)
		}
	}
	return nil
}

func (c *ColdNeonDBClient) Cleanup() error {
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "DROP TABLE users;")
	if err != nil {
		return err
	}

	// give neondb compute time
	time.Sleep(5 * time.Second)

	return nil
}
