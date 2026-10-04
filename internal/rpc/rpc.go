package rpc

import (
	"context"
	"encoding/json"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/raft"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"net"
)

type codec struct{}

func (codec) Name() string                            { return "json" }
func (codec) Marshal(v interface{}) ([]byte, error)   { return json.Marshal(v) }
func (codec) Unmarshal(b []byte, v interface{}) error { return json.Unmarshal(b, v) }
func init()                                           { encoding.RegisterCodec(codec{}) }

type server struct{ n *raft.Node }

// HandlerType must be a pointer to an interface (grpc-go requirement);
// the hand-written wire structs below satisfy it.
type raftServer interface {
	RequestVote(context.Context, *wire.RequestVoteRequest) (*wire.RequestVoteResponse, error)
	AppendEntries(context.Context, *wire.AppendEntriesRequest) (*wire.AppendEntriesResponse, error)
	InstallSnapshot(context.Context, *wire.InstallSnapshotRequest) (*wire.InstallSnapshotResponse, error)
}

func (s *server) RequestVote(c context.Context, r *wire.RequestVoteRequest) (*wire.RequestVoteResponse, error) {
	x := s.n.HandleRequestVote(r.Term, r.CandidateID, r.LastLogIndex, r.LastLogTerm)
	return &wire.RequestVoteResponse{Term: x.Term, VoteGranted: x.VoteGranted}, nil
}
func (s *server) AppendEntries(c context.Context, r *wire.AppendEntriesRequest) (*wire.AppendEntriesResponse, error) {
	x := s.n.HandleAppendEntries(r.Term, r.LeaderID, r.PrevLogIndex, r.PrevLogTerm, r.Entries, r.LeaderCommit)
	return &wire.AppendEntriesResponse{Term: x.Term, Success: x.Success, ConflictIndex: x.ConflictIndex, ConflictTerm: x.ConflictTerm}, nil
}
func (s *server) InstallSnapshot(c context.Context, r *wire.InstallSnapshotRequest) (*wire.InstallSnapshotResponse, error) {
	x := s.n.HandleInstallSnapshot(r.Term, r.LeaderID, r.LastIncludedIndex, r.LastIncludedTerm, r.Data)
	return &wire.InstallSnapshotResponse{Term: x.Term}, nil
}
func Register(g *grpc.Server, n *raft.Node) {
	s := &server{n}
	g.RegisterService(&grpc.ServiceDesc{ServiceName: "raft.RaftService", HandlerType: (*raftServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "RequestVote", Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
		in := new(wire.RequestVoteRequest)
		if e := dec(in); e != nil {
			return nil, e
		}
		return srv.(*server).RequestVote(ctx, in)
	}}, {MethodName: "AppendEntries", Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
		in := new(wire.AppendEntriesRequest)
		if e := dec(in); e != nil {
			return nil, e
		}
		return srv.(*server).AppendEntries(ctx, in)
	}}, {MethodName: "InstallSnapshot", Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
		in := new(wire.InstallSnapshotRequest)
		if e := dec(in); e != nil {
			return nil, e
		}
		return srv.(*server).InstallSnapshot(ctx, in)
	}}}}, s)
}

type client struct{ cc *grpc.ClientConn }

func Dial(ctx context.Context, a string) (raft.Peer, error) {
	cc, e := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return nil, e
	}
	return &client{cc}, nil
}
func (c *client) Close() error { return c.cc.Close() }
func (c *client) RequestVote(ctx context.Context, r *wire.RequestVoteRequest) (*wire.RequestVoteResponse, error) {
	o := new(wire.RequestVoteResponse)
	e := c.cc.Invoke(ctx, "/raft.RaftService/RequestVote", r, o, grpc.ForceCodec(codec{}))
	return o, e
}
func (c *client) AppendEntries(ctx context.Context, r *wire.AppendEntriesRequest) (*wire.AppendEntriesResponse, error) {
	o := new(wire.AppendEntriesResponse)
	e := c.cc.Invoke(ctx, "/raft.RaftService/AppendEntries", r, o, grpc.ForceCodec(codec{}))
	return o, e
}
func (c *client) InstallSnapshot(ctx context.Context, r *wire.InstallSnapshotRequest) (*wire.InstallSnapshotResponse, error) {
	o := new(wire.InstallSnapshotResponse)
	e := c.cc.Invoke(ctx, "/raft.RaftService/InstallSnapshot", r, o, grpc.ForceCodec(codec{}))
	return o, e
}
func Serve(l net.Listener, n *raft.Node) error {
	g := grpc.NewServer()
	Register(g, n)
	return g.Serve(l)
}
