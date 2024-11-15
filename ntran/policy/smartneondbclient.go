package policy

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/rand"
)

type SmartNeonDBClient struct {
	NeonDBClient
	branches          []BranchInfo
	defaultBranchName string
}

func (c *SmartNeonDBClient) addCompute(branchName string) {
	cmd := exec.Command("neon", "branch", "add-compute", branchName, "--type", "read_write")
	c.runNeonCmd(cmd)
}

func (c *SmartNeonDBClient) moveBranchesToTargetHead(targetBranchName string) {
	for _, branch := range c.branches {
		if branch.Name != targetBranchName {
			c.moveBranchToHead(branch.Name, targetBranchName)
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (c *SmartNeonDBClient) renameBranch(oldBranchName string, newBranchName string) {
	cmd := exec.Command(
		"neon", "branch",
		"rename", oldBranchName, newBranchName,
	)
	c.runNeonCmd(cmd)
}

func (c *SmartNeonDBClient) makeBranchDefault(branchName string) {
	cmd := exec.Command(
		"neon", "branch",
		"set-default", branchName,
	)
	c.runNeonCmd(cmd)
	c.defaultBranchName = branchName
}

func (c *SmartNeonDBClient) moveBranchToHead(branchName string, targetBranchName string, arg ...string) {
	args := []string{"branch", "restore", branchName, targetBranchName}
	args = append(args, arg...)
	cmd := exec.Command("neon", args...)
	c.runNeonCmd(cmd)
}

func (c *SmartNeonDBClient) GetName() string {
	return "smartneondb"
}

func (c *SmartNeonDBClient) Scaffold() error {
	err := c.NeonDBClient.Scaffold()
	if err != nil {
		return err
	}
	for i := 0; i < 9; i++ {
		db := fmt.Sprintf("db_%v", i)
		connStr := c.createBranch(db)
		c.branches = append(c.branches, BranchInfo{Name: db, ConnStr: connStr})
		time.Sleep(5 * time.Millisecond)
	}
	// we want to get 10 branches with active compute and one root
	// branch with no compute. this accomplishes that.
	c.moveBranchToHead("main", "db_0", "--preserve-under-name", "oldmain")
	c.moveBranchToHead("main", "oldmain")
	c.renameBranch("main", "db_10")
	c.renameBranch("oldmain", "main")
	c.makeBranchDefault("main")
	c.branches = append(c.branches, BranchInfo{Name: "db_10", ConnStr: c.getConnectionString("db_10")})
	return nil
}

func executeBranchInfo(statement Statement, branchInfo BranchInfo, wg *sync.WaitGroup, ch chan ExecutionResult) {
	defer wg.Done()

	var rows pgx.Rows
	if statement.Command != "" {
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
	} else {
		conn, err := pgx.Connect(context.Background(), branchInfo.ConnStr)
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
		ch <- ExecutionResult{BranchName: branchInfo.Name, Statement: statement, Values: v}
	}
}

func (c *SmartNeonDBClient) Execute(testCases []TestCase) error {
	rand.Seed(uint64(time.Now().UnixNano()))
	for i, testCase := range testCases {
		benchmark := Benchmark{Policy: c.GetName(), TestCase: testCase.Name}
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
			}
		}

		// dummy "consensus" step here -- take a random one.
		idx := rand.Intn(len(results))
		winningBranchName := results[idx].BranchName
		c.makeBranchDefault(winningBranchName)
		c.moveBranchesToTargetHead(winningBranchName)

		benchmark.End()
		benchmark.Log(i)
	}
	return nil
}

func (c *SmartNeonDBClient) Cleanup() error {
	for _, branchName := range c.branches {
		if branchName.Name != c.defaultBranchName {
			c.deleteBranch(branchName.Name)
		}
	}
	c.makeBranchDefault("main")
	c.addCompute("main")
	c.deleteBranch(c.defaultBranchName)
	return nil
}
