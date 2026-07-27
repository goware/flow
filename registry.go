package flow

import (
	"errors"
	"fmt"
	"sort"
)

type definitionKey struct {
	name    string
	version int
}

type runtimeRegistry struct {
	workers      map[definitionKey]erasedWorker
	plans        map[definitionKey]erasedPlan
	coordinators map[definitionKey]erasedCoordinator
	frozen       bool
}

func newRuntimeRegistry() *runtimeRegistry {
	return &runtimeRegistry{
		workers: make(map[definitionKey]erasedWorker), plans: make(map[definitionKey]erasedPlan),
		coordinators: make(map[definitionKey]erasedCoordinator),
	}
}

func (r *Runtime) Register(definitions ...Registration) error {
	if r == nil {
		return newError(ErrInvalid, "register", "runtime", "", "runtime is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lifecycle != runtimeCreated || r.closed {
		return newError(ErrInvalidState, "register", "runtime", "", "registration is frozen")
	}
	if len(definitions) == 0 {
		return nil
	}
	workers := cloneMap(r.registry.workers)
	plans := cloneMap(r.registry.plans)
	coordinators := cloneMap(r.registry.coordinators)
	var invalid, conflicts []error
	for _, registration := range definitions {
		if registration == nil {
			invalid = append(invalid, errors.New("nil registration"))
			continue
		}
		data := registration.flowRegistration()
		if data.validation != nil {
			invalid = append(invalid, fmt.Errorf("%s %s/%d: %w", data.kind, data.name, data.version, data.validation))
			continue
		}
		key := definitionKey{name: data.name, version: data.version}
		switch data.kind {
		case workerRegistrationKind:
			value, ok := data.value.(erasedWorker)
			if !ok || value.command == nil || value.invoke == nil {
				invalid = append(invalid, fmt.Errorf("worker %s/%d is incomplete", data.name, data.version))
				continue
			}
			if _, exists := workers[key]; exists {
				conflicts = append(conflicts, fmt.Errorf("duplicate worker %s/%d", data.name, data.version))
				continue
			}
			workers[key] = value
		case planRegistrationKind:
			value, ok := data.value.(erasedPlan)
			if !ok || value.def == nil || value.invoke == nil {
				invalid = append(invalid, fmt.Errorf("plan %s/%d is incomplete", data.name, data.version))
				continue
			}
			if _, exists := plans[key]; exists {
				conflicts = append(conflicts, fmt.Errorf("duplicate plan %s/%d", data.name, data.version))
				continue
			}
			plans[key] = value
		case coordinatorRegistrationKind:
			value, ok := data.value.(erasedCoordinator)
			if !ok || value.stateDef == nil {
				invalid = append(invalid, fmt.Errorf("coordinator %s/%d is incomplete", data.name, data.version))
				continue
			}
			if _, exists := coordinators[key]; exists {
				conflicts = append(conflicts, fmt.Errorf("duplicate coordinator %s/%d", data.name, data.version))
				continue
			}
			coordinators[key] = value
		default:
			invalid = append(invalid, fmt.Errorf("unknown registration kind %q", data.kind))
		}
	}
	if err := errors.Join(invalid...); err != nil {
		return newError(ErrInvalid, "register", "definitions", "", err.Error())
	}
	if err := errors.Join(conflicts...); err != nil {
		return newError(ErrConflict, "register", "definitions", "", err.Error())
	}
	r.registry.workers, r.registry.plans, r.registry.coordinators = workers, plans, coordinators
	return nil
}

func (registry *runtimeRegistry) freeze() {
	registry.workers = cloneMap(registry.workers)
	registry.plans = cloneMap(registry.plans)
	registry.coordinators = cloneMap(registry.coordinators)
	registry.frozen = true
}

func (registry *runtimeRegistry) worker(name string, version int) (erasedWorker, bool) {
	worker, ok := registry.workers[definitionKey{name: name, version: version}]
	return worker, ok
}

func (registry *runtimeRegistry) workerKeys() []definitionKey {
	result := make([]definitionKey, 0, len(registry.workers))
	for key := range registry.workers {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].name == result[j].name {
			return result[i].version < result[j].version
		}
		return result[i].name < result[j].name
	})
	return result
}

func (registry *runtimeRegistry) plan(name string, version int) (erasedPlan, bool) {
	plan, ok := registry.plans[definitionKey{name: name, version: version}]
	return plan, ok
}

func (registry *runtimeRegistry) planKeys() []definitionKey {
	result := make([]definitionKey, 0, len(registry.plans))
	for key := range registry.plans {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].name == result[j].name {
			return result[i].version < result[j].version
		}
		return result[i].name < result[j].name
	})
	return result
}

func (registry *runtimeRegistry) coordinator(name string, version int) (erasedCoordinator, bool) {
	value, ok := registry.coordinators[definitionKey{name: name, version: version}]
	return value, ok
}

func (registry *runtimeRegistry) coordinatorKeys() []definitionKey {
	result := make([]definitionKey, 0, len(registry.coordinators))
	for key := range registry.coordinators {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].name == result[j].name {
			return result[i].version < result[j].version
		}
		return result[i].name < result[j].name
	})
	return result
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
