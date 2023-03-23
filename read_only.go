// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "go.etcd.io/raft/v3/raftpb"

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
// 记录每个请求对应的readIndex值，这个rctx在readOnly结构体中
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct {
	// req 记录每个readIndex rctx对应的原消息
	req   pb.Message
	// 需要确认的readIndex 值
	index uint64
	// NB: this never records 'false', but it's more convenient to use this
	// instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
	// this becomes performance sensitive enough (doubtful), quorum.VoteResult
	// can change to an API that is closer to that of CommittedIndex.
	// 已经收到的确认该read index的响应，map的key是node id
	acks map[uint64]bool
}

// readOnly结构体是leader仅使用ReadIndex时，用来记录等待心跳确认的read index的结构体
type readOnly struct {
	// 读方式的参数，采用readIndex读还是 lease 读
	option           ReadOnlyOption

	//如果采用lease读，下面两个字段不会有数据
	//pendingReadIndex是rctx到其相应的状态readIndexStatus的映射。
	//readIndexStatus结构体的req字段记录了该rctx对应的原消息（在发送该消息的响应时需要用到），
	//index字段记录了待确认的read index的值，ack字段记录了已收到的确认该read index的心跳响应。
	pendingReadIndex map[string]*readIndexStatus
	//多次调用readIndex产生rctx参数队列，每个readIndex请求都会有一个唯一的rctx
	readIndexQueue   []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only request into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	s := string(m.Entries[0].Data)
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]bool)}
	ro.readIndexQueue = append(ro.readIndexQueue, s)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]bool {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}

	rs.acks[id] = true
	return rs.acks
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
// 如果某个readIndex满足quorum要求，调用这个方法会返回相应readState,readState记录index值和对应请求rctx，这个是一一对应的
//
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context)
	var rss []*readIndexStatus

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)
		if okctx == ctx {
			found = true
			break
		}
	}

	// 因为是批量操作，之前readIndex返回，只有也会返回
	if found {
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
// 获取readIndex最后一条记录，在心跳超时，leader会触发心跳广播，
// 这样如果携带最后一个rctx,这个readIndex都会返回
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
