package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"golang.org/x/exp/rand"
)

type NeonDBClient struct {
	mainConnStr string
}

type BranchInfo struct {
	Name    string
	ConnStr string
}

type ExecutionResult struct {
	Statement Statement
	Values    []any
	Error     error
}

func (c *NeonDBClient) GetName() string {
	return "neondb"
}

func (c *NeonDBClient) Scaffold() error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error loading .env file")
	}
	c.mainConnStr = os.Getenv("NEON_DATABASE_URL")
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

func (c *NeonDBClient) GenerateSQL(inFlight int) ([]TestCase, error) {

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

func (c *NeonDBClient) deleteBranch(name string) {
	maxAttempts := 3
	var stdout strings.Builder
	var stderr strings.Builder
	for attempts := 0; attempts < maxAttempts; attempts++ {
		cmd := exec.Command(
			"neon", "branches", "delete", name,
			"--output", "json",
		)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			if strings.Contains(err.Error(), "ERROR: Request timed out") {
				if attempts == maxAttempts {
					log.Default().Println(stderr.String())
					log.Fatal(err)
				} else {
					time.Sleep(1 * time.Second)
					continue
				}
			}
		}
	}
}

/*
 * createBranch - creates neondb branch. TODO: takes on the order of seconds, so might be
 * worth creating a bunch up front. then as we get closer to the number of transactions
 * we want to run, start to spawn more branches.
 */
func (c *NeonDBClient) createBranch(name string) string {

	maxAttempts := 3
	var stdout strings.Builder
	var stderr strings.Builder
	for attempts := 0; attempts < maxAttempts; attempts++ {
		cmd := exec.Command(
			"neon", "branches", "create",
			"--name", name,
			"--output", "json",
		)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			// https://community.neon.tech/t/project-already-has-running-operations-scheduling-of-new-ones-is-prohibited/242
			concurrentErrorMessage := "ERROR: project already has running operations, scheduling of new ones is prohibited"
			if strings.Contains(err.Error(), concurrentErrorMessage) {
				if attempts == maxAttempts {
					log.Default().Println(stderr.String())
					log.Fatal(err)
				} else {
					time.Sleep(1 * time.Second)
					continue
				}
			}
		}
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout.String()), &result); err != nil {
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

/*
 * commit - commits the selected change to the database. after this, we delete all the branches.
 * TODO: however, we could run `neon branch <branch-name> restore ^<winning-branch-name>` to set all
 * branches up to the state of the branch that has already won.
 */
func (c *NeonDBClient) commit(statement Statement) error {
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
	if statement.Command != "" {
		if branchInfo, ok := branchInfoMap[statement.Command]; ok {
			conn, err := pgx.Connect(context.Background(), branchInfo.ConnStr)
			if err != nil {
				ch <- ExecutionResult{Error: err}
			}
			defer conn.Close(context.Background())

			_, err = conn.Exec(context.Background(), statement.Command)
			if err != nil {
				ch <- ExecutionResult{Error: err}
			}

			rows, err = conn.Query(context.Background(), statement.Query)
			if err != nil {
				ch <- ExecutionResult{Error: err}
			}
		}
	} else {
		conn, err := pgx.Connect(context.Background(), mainConnStr)
		if err != nil {
			ch <- ExecutionResult{Error: err}
		}
		defer conn.Close(context.Background())

		rows, err = conn.Query(context.Background(), statement.Query)
		if err != nil {
			ch <- ExecutionResult{Error: err}
		}
	}
	if rows.Next() {
		v, err := rows.Values()
		if err != nil {
			ch <- ExecutionResult{Error: err}
		}
		ch <- ExecutionResult{Statement: statement, Values: v}
	}
}

func (c *NeonDBClient) Execute(testCases []TestCase) error {
	rand.Seed(uint64(time.Now().UnixNano()))
	for i, testCase := range testCases {
		benchmark := Benchmark{Policy: c.GetName(), TestCase: testCase.Name}
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
		benchmark.Log(i)

		for _, branchInfo := range branchInfoMap {
			c.deleteBranch(branchInfo.Name)
		}
	}
	return nil
}

func (c *NeonDBClient) Cleanup() error {
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
