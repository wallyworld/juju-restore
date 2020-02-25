// Copyright 2020 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package core_test

import (
	"regexp"

	"github.com/juju/errors"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/juju-restore/core"
	"github.com/juju/juju-restore/machine"
)

type restorerSuite struct {
	testing.IsolationSuite
	converter func(member core.ReplicaSetMember) core.ControllerNode
}

var _ = gc.Suite(&restorerSuite{})

func (s *restorerSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.converter = machine.ControllerNodeForReplicaSetMember
}

func (s *restorerSuite) TestCheckDatabaseStateUnhealthyMembers(c *gc.C) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{{
					Healthy:       false,
					ID:            1,
					Name:          "kaira-ba",
					State:         "SECONDARY",
					JujuMachineID: "0",
				}, {
					Healthy:       true,
					ID:            2,
					Name:          "djula",
					State:         "PRIMARY",
					JujuMachineID: "1",
				}, {
					Healthy:       true,
					ID:            3,
					Name:          "bibi",
					State:         "OUCHY",
					JujuMachineID: "2",
				}},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	err = r.CheckDatabaseState()
	c.Assert(err, jc.Satisfies, core.IsUnhealthyMembersError)
	c.Assert(err, gc.ErrorMatches, regexp.QuoteMeta(`unhealthy replica set members: 1 "kaira-ba" (juju machine 0), 3 "bibi" (juju machine 2)`))
}

func (s *restorerSuite) TestCheckDatabaseStateNoPrimary(c *gc.C) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{{
					Healthy:       true,
					ID:            1,
					Name:          "kaira-ba",
					State:         "SECONDARY",
					JujuMachineID: "2",
				}, {
					Healthy:       true,
					ID:            2,
					Name:          "djula",
					State:         "SECONDARY",
					JujuMachineID: "1",
				}, {
					Healthy:       true,
					ID:            3,
					Name:          "bibi",
					State:         "SECONDARY",
					JujuMachineID: "0",
				}},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	err = r.CheckDatabaseState()
	c.Assert(err, gc.ErrorMatches, "no primary found in replica set")
}

func (s *restorerSuite) TestCheckDatabaseStateNotPrimary(c *gc.C) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{{
					Healthy:       true,
					ID:            1,
					Name:          "kaira-ba",
					State:         "SECONDARY",
					Self:          true,
					JujuMachineID: "1",
				}, {
					Healthy:       true,
					ID:            2,
					Name:          "djula",
					State:         "PRIMARY",
					JujuMachineID: "2",
				}, {
					Healthy:       true,
					ID:            3,
					Name:          "bibi",
					State:         "SECONDARY",
					JujuMachineID: "0",
				}},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	err = r.CheckDatabaseState()
	c.Assert(err, gc.ErrorMatches, regexp.QuoteMeta(`not running on primary replica set member, primary is 2 "djula" (juju machine 2)`))
}

func (s *restorerSuite) TestCheckDatabaseStateAllGood(c *gc.C) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{{
					Healthy:       true,
					ID:            1,
					Name:          "kaira-ba",
					State:         "SECONDARY",
					JujuMachineID: "0",
				}, {
					Healthy:       true,
					ID:            2,
					Name:          "djula",
					State:         "PRIMARY",
					Self:          true,
					JujuMachineID: "1",
				}, {
					Healthy:       true,
					ID:            3,
					Name:          "bibi",
					State:         "SECONDARY",
					JujuMachineID: "2",
				}},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	err = r.CheckDatabaseState()
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(r.IsHA(), jc.IsTrue)
}

func (s *restorerSuite) TestCheckDatabaseStateOneMember(c *gc.C) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{{
					Healthy:       true,
					ID:            2,
					Name:          "djula",
					State:         "PRIMARY",
					Self:          true,
					JujuMachineID: "2",
				}},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	err = r.CheckDatabaseState()
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(r.IsHA(), jc.IsFalse)
}

func (s *restorerSuite) TestCheckDatabaseStateMissingJujuID(c *gc.C) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{{
					Healthy: true,
					ID:      2,
					Name:    "djula",
					State:   "PRIMARY",
					Self:    true,
				}},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	err = r.CheckDatabaseState()
	c.Assert(err, gc.ErrorMatches, regexp.QuoteMeta(`unhealthy replica set members: 2 "djula" (juju machine )`))
}

func (s *restorerSuite) TestCheckSecondaryControllerNodesSkipsSelf(c *gc.C) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{
					{
						Healthy:       true,
						ID:            2,
						Name:          "djula:wot",
						State:         "PRIMARY",
						Self:          true,
						JujuMachineID: "2",
					},
				},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(r.CheckSecondaryControllerNodes(), gc.DeepEquals, map[string]error{})
}

func (s *restorerSuite) checkSecondaryControllerNodes(c *gc.C, expected map[string]error) {
	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{
					{
						Healthy:       true,
						ID:            2,
						Name:          "djula",
						State:         "PRIMARY",
						Self:          true,
						JujuMachineID: "2",
					},
					{
						Healthy:       true,
						ID:            1,
						Name:          "wot",
						State:         "SECONDARY",
						JujuMachineID: "1",
					},
				},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(r.CheckSecondaryControllerNodes(), gc.DeepEquals, expected)
}

func (s *restorerSuite) TestCheckSecondaryControllerNodesOk(c *gc.C) {
	s.converter = func(member core.ReplicaSetMember) core.ControllerNode {
		return &fakeControllerNode{Stub: &testing.Stub{}, ip: member.Name}
	}
	s.checkSecondaryControllerNodes(c, map[string]error{"wot": nil})
}

func (s *restorerSuite) TestCheckSecondaryControllerNodesFail(c *gc.C) {
	err := errors.New("boom")
	s.converter = func(member core.ReplicaSetMember) core.ControllerNode {
		node := &fakeControllerNode{Stub: &testing.Stub{}, ip: member.Name}
		node.SetErrors(err)
		return node
	}
	s.checkSecondaryControllerNodes(c, map[string]error{"wot": err})
}

type agentMgmtTest struct {
	mgmtFunc    func(*core.Restorer, bool) map[string]error
	secondaries bool
	result      map[string]error
	nodeErrs    map[string]string
}

func (s *restorerSuite) checkManagedAgents(c *gc.C, t agentMgmtTest) []*fakeControllerNode {
	nodes := []*fakeControllerNode{}
	s.converter = func(member core.ReplicaSetMember) core.ControllerNode {
		node := &fakeControllerNode{Stub: &testing.Stub{}, ip: member.Name}
		nodes = append(nodes, node)
		if e := t.nodeErrs[member.Name]; e != "" {
			node.SetErrors(errors.New(e))
		}
		return node
	}

	r, err := core.NewRestorer(&fakeDatabase{
		replicaSetF: func() (core.ReplicaSet, error) {
			return core.ReplicaSet{
				Members: []core.ReplicaSetMember{
					{
						Healthy:       true,
						ID:            2,
						Name:          "djula",
						State:         "PRIMARY",
						Self:          true,
						JujuMachineID: "2",
					},
					{
						Healthy:       true,
						ID:            1,
						Name:          "wot",
						State:         "SECONDARY",
						JujuMachineID: "1",
					},
				},
			}, nil
		},
	}, s.converter)
	c.Assert(err, jc.ErrorIsNil)

	result := t.mgmtFunc(r, t.secondaries)
	c.Assert(len(result), gc.Equals, len(t.result))
	for k, v := range result {
		if v != nil {
			c.Assert(v, gc.ErrorMatches, t.result[k].Error())
		} else {
			c.Assert(v, jc.ErrorIsNil)
		}
	}
	return nodes
}

func (s *restorerSuite) TestStopAgentsWithSecondaries(c *gc.C) {
	nodes := s.checkManagedAgents(c, agentMgmtTest{
		func(r *core.Restorer, s bool) map[string]error { return r.StopAgents(s) },
		true,
		map[string]error{
			"wot":   nil,
			"djula": nil,
		},
		map[string]string{},
	})
	c.Assert(nodes, gc.HasLen, 2)
	for _, n := range nodes {
		n.CheckCallNames(c, "IP", "StopAgent")
	}
}

func (s *restorerSuite) TestStopAgentsNoSecondaries(c *gc.C) {
	nodes := s.checkManagedAgents(c, agentMgmtTest{
		func(r *core.Restorer, s bool) map[string]error { return r.StopAgents(s) },
		false,
		map[string]error{
			"djula": nil,
		},
		map[string]string{},
	})
	c.Assert(nodes, gc.HasLen, 2)
	for _, n := range nodes {
		// When no secondaries are requested, only primary node will be run
		if n.IP() == "djula" {
			n.CheckCallNames(c, "IP", "StopAgent", "IP")
		} else {
			n.CheckCallNames(c, "IP")
		}
	}
}

func (s *restorerSuite) TestStopAgentFail(c *gc.C) {
	s.checkManagedAgents(c, agentMgmtTest{
		func(r *core.Restorer, s bool) map[string]error { return r.StopAgents(s) },
		true,
		map[string]error{
			"djula": errors.New("kaboom"),
			"wot":   nil,
		},
		map[string]string{"djula": "kaboom"},
	})
}

func (s *restorerSuite) TestStartAgentsWithSecondaries(c *gc.C) {
	nodes := s.checkManagedAgents(c, agentMgmtTest{
		func(r *core.Restorer, s bool) map[string]error { return r.StartAgents(s) },
		true,
		map[string]error{
			"wot":   nil,
			"djula": nil,
		},
		map[string]string{},
	})
	c.Assert(nodes, gc.HasLen, 2)
	for _, n := range nodes {
		n.CheckCallNames(c, "IP", "StartAgent")
	}
}

func (s *restorerSuite) TestStartAgentsNoSecondaries(c *gc.C) {
	nodes := s.checkManagedAgents(c, agentMgmtTest{
		func(r *core.Restorer, s bool) map[string]error { return r.StartAgents(s) },
		false,
		map[string]error{
			"djula": nil,
		},
		map[string]string{},
	})
	c.Assert(nodes, gc.HasLen, 2)
	for _, n := range nodes {
		// When no secondaries are requested, only primary node will be run
		if n.IP() == "djula" {
			n.CheckCallNames(c, "IP", "StartAgent", "IP")
		} else {
			n.CheckCallNames(c, "IP")
		}
	}
}

func (s *restorerSuite) TestStartAgentFail(c *gc.C) {
	s.checkManagedAgents(c, agentMgmtTest{
		func(r *core.Restorer, s bool) map[string]error { return r.StartAgents(s) },
		true,
		map[string]error{
			"wot":   errors.New("kaboom"),
			"djula": nil,
		},
		map[string]string{"wot": "kaboom"},
	})
}

type fakeDatabase struct {
	testing.Stub
	replicaSetF func() (core.ReplicaSet, error)
}

func (db *fakeDatabase) ReplicaSet() (core.ReplicaSet, error) {
	db.Stub.MethodCall(db, "ReplicaSet")
	return db.replicaSetF()
}

func (db *fakeDatabase) Close() {
	db.Stub.MethodCall(db, "Close")
}

type fakeControllerNode struct {
	*testing.Stub
	ip string
}

func (f *fakeControllerNode) IP() string {
	f.Stub.MethodCall(f, "IP")
	return f.ip
}

func (f *fakeControllerNode) Ping() error {
	f.Stub.MethodCall(f, "Ping")
	return f.NextErr()
}

func (f *fakeControllerNode) StopAgent() error {
	f.Stub.MethodCall(f, "StopAgent")
	return f.NextErr()
}

func (f *fakeControllerNode) StartAgent() error {
	f.Stub.MethodCall(f, "StartAgent")
	return f.NextErr()
}
