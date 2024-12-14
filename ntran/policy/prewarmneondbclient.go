package policy

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/rand"
)

type PreWarmNeonDBClient struct {
	ColdNeonDBClient
	branches          []BranchInfo
	defaultBranchName string
}

func (c *PreWarmNeonDBClient) addCompute(branchName string) {
	c.runNeonCmd("read_write endpoint already exists", "branch", "add-compute", branchName, "--type", "read_write")
}

func (c *PreWarmNeonDBClient) moveBranchesToTargetHead(targetBranchName string) {
	for _, branch := range c.branches {
		if branch.Name != targetBranchName {
			c.moveBranchToHead(branch.Name, targetBranchName)
		}
	}
}

func (c *PreWarmNeonDBClient) renameBranch(oldBranchName string, newBranchName string) {
	c.runNeonCmd(fmt.Sprintf("Branch %s not found", oldBranchName), "branch", "rename", oldBranchName, newBranchName)
}

func (c *PreWarmNeonDBClient) makeBranchDefault(branchName string) {
	c.runNeonCmd("", "branch", "set-default", branchName)
	c.defaultBranchName = branchName
}

func (c *PreWarmNeonDBClient) moveBranchToHead(branchName string, targetBranchName string, arg ...string) {
	args := []string{"branch", "restore", branchName, targetBranchName}
	args = append(args, arg...)
	c.runNeonCmd("", args...)
}

func (c *PreWarmNeonDBClient) GetName() string {
	return "prewarm-neondb"
}

func (c *PreWarmNeonDBClient) GetNumTransactionsInFlight() []int {
	return []int{2, 4, 6, 7, 8}
}

func (c *PreWarmNeonDBClient) Scaffold(schema string, inFlight int) error {
	err := c.ColdNeonDBClient.Scaffold(schema, inFlight)
	if err != nil {
		return err
	}
	if inFlight > 10 {
		log.Fatalf("error scaffolding smartneondb. Can only handle 10 concurrent branches, so inFlight must be at most 10")
	}

	/*
	 * 1. create (inFlight-1) extra branches
	 * 2. turn main into the last branch with active compute
	 * 3. have an archived branch (with no compute) as the parent to all branches
	 */
	for i := 0; i < inFlight-1; i++ {
		db := fmt.Sprintf("db_%v", i)
		connStr := c.createBranch(db)
		c.branches = append(c.branches, BranchInfo{Name: db, ConnStr: connStr})
	}
	lastdb := fmt.Sprintf("db_%v", inFlight)
	c.moveBranchToHead("main", "db_0", "--preserve-under-name", "oldmain")
	c.moveBranchToHead("main", "oldmain")
	c.renameBranch("main", lastdb)
	c.renameBranch("oldmain", "main")
	c.makeBranchDefault("main")
	c.branches = append(c.branches, BranchInfo{Name: lastdb, ConnStr: c.getConnectionString(lastdb)})
	return nil
}

func executeBranchInfo(statement Statement, branchInfo BranchInfo, wg *sync.WaitGroup, ch chan ExecutionResult) {
	defer wg.Done()

	var rows pgx.Rows
	var values []any

	conn, err := pgx.Connect(context.Background(), branchInfo.ConnStr)
	if err != nil {
		ch <- ExecutionResult{Error: err}
		return
	}
	defer conn.Close(context.Background())

	if statement.Command != "" {

		_, err := conn.Exec(context.Background(), statement.Command)
		if err != nil {
			ch <- ExecutionResult{Error: err}
			return
		}
	} else {
		rows, err = conn.Query(context.Background(), statement.Query)
		if err != nil {
			ch <- ExecutionResult{Error: err}
			return
		}
		if rows.Next() {
			values, err = rows.Values()
			if err != nil {
				ch <- ExecutionResult{Error: err}
				return
			}
		}
	}

	ch <- ExecutionResult{BranchName: branchInfo.Name, Statement: statement, Values: values}
}

func (c *PreWarmNeonDBClient) Execute(testCase TestCase, experiment *Experiment) error {
	rand.Seed(uint64(time.Now().UnixNano()))
	benchmark := Benchmark{
		Experiment:       experiment,
		Policy:           c.GetName(),
		TestCase:         testCase.Name,
		TransactionCount: len(testCase.Statements),
	}
	benchmark.Start()

	var results []ExecutionResult
	ch := make(chan ExecutionResult)
	var wg sync.WaitGroup

	for i, statement := range testCase.Statements {
		wg.Add(1)
		branchInfo := c.branches[i]
		go executeBranchInfo(statement, branchInfo, &wg, ch)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for result := range ch {
		if result.Error == nil {
			results = append(results, result)
		} else {
			log.Printf("error encountered while executing statement: %v", result.Error)
		}
	}

	// dummy "consensus" step here -- take a random one.
	idx := rand.Intn(len(results))
	winningBranchName := results[idx].BranchName
	c.makeBranchDefault(winningBranchName)
	c.moveBranchesToTargetHead(winningBranchName)

	benchmark.End()
	benchmark.Log()
	return nil
}

func (c *PreWarmNeonDBClient) Cleanup(sql string) error {
	currDefaultBranchName := c.defaultBranchName
	for _, branchName := range c.branches {
		if branchName.Name != currDefaultBranchName {
			c.deleteBranch(branchName.Name)
		}
	}

	c.makeBranchDefault("main")
	c.addCompute("main")

	c.deleteBranch(currDefaultBranchName)
	c.branches = []BranchInfo{}

	c.mainConnStr = c.getConnectionString("main")
	return c.ColdNeonDBClient.Cleanup(sql)
}
