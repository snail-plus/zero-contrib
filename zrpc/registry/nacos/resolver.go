package nacos

import (
	"context"
	"fmt"
	"google.golang.org/grpc/attributes"
	"sort"

	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/resolver"
)

type resolvr struct {
	cancelFunc context.CancelFunc
}

func (r *resolvr) ResolveNow(resolver.ResolveNowOptions) {}

// Close closes the resolver.
func (r *resolvr) Close() {
	r.cancelFunc()
}

type watcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	out    chan<- []model.Instance
}

func newWatcher(ctx context.Context, cancel context.CancelFunc, out chan<- []model.Instance) *watcher {
	return &watcher{
		ctx:    ctx,
		cancel: cancel,
		out:    out,
	}
}

func (nw *watcher) CallBackHandle(services []model.Instance, err error) {
	if err != nil {
		logger.Error("[Nacos resolver] watcher call back handle error:%v", err)
		return
	}
	nw.out <- services
}

func populateEndpoints(ctx context.Context, clientConn resolver.ClientConn, input <-chan []model.Instance) {
	for {
		select {
		case cc := <-input:
			connsSet := make(map[string]model.Instance, len(cc))
			for _, c := range cc {
				addr := fmt.Sprintf("%s:%d", c.Ip, c.Port)
				connsSet[addr] = c
			}
			conns := make([]resolver.Address, 0, len(connsSet))
			for k, v := range connsSet {
				metadata := v.Metadata
				var attribute *attributes.Attributes
				for key, value := range metadata {
					attribute = attribute.WithValue(key, value)
				}

				conns = append(conns, resolver.Address{
					Addr:       k,
					ServerName: v.ServiceName,
					Attributes: attribute,
				})
			}
			sort.Sort(byAddressString(conns)) // Don't replace the same address list in the balancer
			_ = clientConn.UpdateState(resolver.State{Addresses: conns})
		case <-ctx.Done():
			logx.Info("[Nacos resolver] Watch has been finished")
			return
		}
	}
}

// byAddressString sorts resolver.Address by Address Field  sorting in increasing order.
type byAddressString []resolver.Address

func (p byAddressString) Len() int           { return len(p) }
func (p byAddressString) Less(i, j int) bool { return p[i].Addr < p[j].Addr }
func (p byAddressString) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
