# Lab 2: Raft - How to Test

Run every command from the project root (the folder that contains `go.mod`).
The commands are written for PowerShell (VS Code terminal on Windows).
Lines starting with `#` are comments and can be pasted as they are.

---

## 1. Preparation checks

```powershell
# Show the installed Go version (needs Go 1.22 or newer)
go version

# Check formatting: prints nothing if all files are correctly formatted
gofmt -l raft

# Static analysis of the Raft package: prints nothing if there are no issues
go vet ./raft

# Check that the whole project compiles
go build ./...
```

---

## 2. Checkpoint: leader election and heartbeats (3A)

```powershell
# Run both election tests:
#   TestInitialElection3A - a single leader is elected and the term stays stable without failures
#   TestReElection3A      - a new leader is elected after the leader disconnects, an old leader
#                           that rejoins does not disturb the cluster, and no leader exists
#                           without a majority
go test -v -run 3A ./raft
```

---

## 3. Final milestone: log replication (3B)

```powershell
# Run all replication tests:
#   TestBasicAgree3B      - commands are committed at the right indexes on all servers
#   TestFailAgree3B       - agreement continues with a disconnected follower, which catches up when it reconnects
#   TestFailNoAgree3B     - nothing is committed without a majority of servers
#   TestRejoin3B          - a partitioned old leader's uncommitted entries are replaced by the new leader's log
#   TestBackup3B          - the leader quickly repairs followers with badly diverged logs (slowest test)
#   TestCount3B           - RPC volume stays low (heartbeat rate and RPCs per entry within limits)
go test -v -run 3B ./raft

# Run a single test on its own (example: the log-repair test)
go test -v -run TestBackup3B ./raft
```

---

## 4. Full test suite

```powershell
# Run every test in the project, including the supplied labrpc tests
# Expected: two "ok" lines (lab2raft/labrpc and lab2raft/raft)
go test ./...

# Optional: repeat the whole suite 3 times to show the results are stable and not a lucky run
go test -count=3 ./...
```

---

## 5. Race detector

```powershell
# Enable cgo for this terminal session (needs gcc installed; repeat in every new terminal)
$env:CGO_ENABLED=1

# Run all tests with the race detector:
# it reports "WARNING: DATA RACE" if goroutines access shared state without proper locking
# Expected: two "ok" lines and no race warnings
go test -race ./...
```

---

## 6. Unreliable network (dropped, delayed and reordered messages)

Do this on a throwaway copy so the real submission is never modified.

```powershell
# Go to the parent folder
cd ..

# Make a throwaway copy of the project
Copy-Item raft-go raft-go-unreliable -Recurse

# Enter the copy
cd raft-go-unreliable

# Switch every test in the copy to the unreliable network (make_config(..., true))
(Get-Content raft\test_test.go) -replace 'make_config\(t, servers, false\)','make_config(t, servers, true)' | Set-Content raft\test_test.go

# Verify the edit: all 8 lines must end with "true)"
Select-String -Path raft\test_test.go -Pattern "make_config"

# Run all Raft tests while the network loses and delays messages
# Expected: all 8 tests pass
go test -v ./raft

# Same run with the race detector enabled
$env:CGO_ENABLED=1
go test -race -v ./raft

# Clean up: leave the copy and delete it
cd ..
Remove-Item raft-go-unreliable -Recurse -Force

# Return to the real project folder
cd raft-go
```

---

## Expected results summary

| Step | Expected result |
|------|-----------------|
| `gofmt`, `go vet ./raft`, `go build` | No output |
| 3A tests | 2 tests pass |
| 3B tests | 6 tests pass |
| `go test ./...` | `ok` for `labrpc` and `raft` |
| `go test -race ./...` | `ok` for both, no `DATA RACE` warnings |
| Unreliable network | All 8 Raft tests pass, with and without `-race` |

Note: `go vet ./...` (without `./raft`) prints warnings about the supplied
`labrpc/test_test.go`. That file belongs to the course and must not be modified.
