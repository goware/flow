package flow

import (
	"errors"

	"github.com/goware/flow/internal/definition"
)

type erasedPlan struct {
	def    *definition.Plan
	invoke func(*Plan, any) error
}

func (p PlanDef[A]) flowRegistration() registrationData {
	name, version := "", 0
	if p.def != nil {
		name, version = p.def.Name, p.def.Version
	}
	erased := erasedPlan{def: p.def}
	if p.invoke != nil {
		erased.invoke = func(plan *Plan, args any) error {
			typed, ok := args.(A)
			if !ok {
				return newError(ErrInvalid, "plan", "execution", name, "argument type mismatch")
			}
			p.invoke(plan, typed)
			return nil
		}
	}
	validation := p.err
	if p.def == nil {
		validation = errors.Join(validation, errors.New("zero plan definition"))
	}
	return registrationData{
		kind: planRegistrationKind, name: name, version: version,
		value: erased, validation: validation,
	}
}
