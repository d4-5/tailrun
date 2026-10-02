package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"

	"uuid"
)

const (
	maxWorkerNameBytes  = 64
	workerStateFilename = "worker-state.json"
)

type WorkerState struct {
	WorkerID uuid.UUID `json:"workerId"`
	Name     string    `json:"name"`
}

var workerNameAdjectives = []string{
	"amber", "brisk", "copper", "crimson", "frosty", "golden", "hidden", "midnight", "quiet", "rotary",
}

var workerNameNouns = []string{
	"compass", "circuit", "meadow", "orbit", "telephone", "lantern", "harbor", "pioneer", "workshop", "signal",
}

func OpenWorkerState(dataDir, name string) (WorkerState, error) {
	name = strings.TrimSpace(name)
	if name != "" {
		if err := validateWorkerName(name); err != nil {
			return WorkerState{}, err
		}
	}

	statePath := filepath.Join(dataDir, workerStateFilename)
	data, err := os.ReadFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		resolvedName := name
		if name == "" {
			resolvedName = generateWorkerName()
		}

		state := WorkerState{
			WorkerID: uuid.NewV4(),
			Name:     resolvedName,
		}
		if err := saveWorkerState(statePath, state); err != nil {
			return WorkerState{}, err
		}
		return state, nil
	}

	if err != nil {
		return WorkerState{}, fmt.Errorf("read state file: %w", err)
	}

	var state WorkerState
	if err := json.Unmarshal(data, &state); err != nil {
		return WorkerState{}, fmt.Errorf("decode state file: %w", err)
	}

	if state.WorkerID == uuid.Nil() {
		return WorkerState{}, errors.New("state file has a nil worker ID")
	}

	savedName := state.Name
	state.Name = strings.TrimSpace(state.Name)
	if err := validateWorkerName(state.Name); err != nil {
		return WorkerState{}, fmt.Errorf("invalid saved worker name: %w", err)
	}

	stateChanged := state.Name != savedName
	if name != "" && state.Name != name {
		state.Name = name
		stateChanged = true
	}

	if stateChanged {
		if err := saveWorkerState(statePath, state); err != nil {
			return WorkerState{}, err
		}
	}

	return state, nil
}

func saveWorkerState(path string, state WorkerState) error {
	data, err := json.MarshalIndent(state, "", "\t")
	if err != nil {
		return fmt.Errorf("encode state file: %w", err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), ".worker-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary state file: %w", err)
	}
	tempPath := tempFile.Name()
	defer func() { _ = os.Remove(tempPath) }()

	if _, err := tempFile.Write(data); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("write state file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("sync state file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close state file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace state file: %w", err)
	}
	return nil
}

func validateWorkerName(name string) error {
	if name == "" {
		return errors.New("name is required")
	}
	if len(name) > maxWorkerNameBytes {
		return fmt.Errorf("name must be at most %d bytes", maxWorkerNameBytes)
	}
	return nil
}

func generateWorkerName() string {
	return fmt.Sprintf(
		"%s-%s",
		workerNameAdjectives[rand.IntN(len(workerNameAdjectives))],
		workerNameNouns[rand.IntN(len(workerNameNouns))],
	)
}
