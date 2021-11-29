package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// 整个DB就是个带读写锁的map
// map中的key-value都带有一些信息,DB的增删改查就是map的增删改查
type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

// RegistrationDB 的 key
type Registration struct {
	Category string
	Key      string
	SubKey   string
}

func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && key != k.Key {
		return false
	}
	if subkey != "*" && key != k.Key {
		return false
	}
	return true
}

type Registrations []Registration

func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

type Producer struct {
	peerInfo    *PeerInfo
	tombstoned  bool
	tombstoneAt time.Time
}

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstoneAt = time.Now()
}

func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstoneAt) < lifetime
}

type Producers []*Producer

func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		// 失去心跳超过inactivityTimeout || 死亡时间 < tombstoneLifetime
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

// RegistrationDB 的 value
type ProducerMap map[string]*Producer

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	produces := r.registrationMap[k]
	_, fount := produces[p.peerInfo.id]
	if !fount {
		produces[p.peerInfo.id] = p
	}
	return !fount
}

func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}
	delete(producers, id)
	return removed, len(producers)
}

func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

// RegistrationDB 支持精确查找,还支持*通配符实现模糊查找
func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// 根据key 找Registrations
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	r.RUnlock()
	// 精确查找, Registrations中只可能有一个Registration
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	// 模糊查找, Registrations中可能有多个Registration
	result := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		result = append(result, k)
	}
	return result
}

// 根据key 找Producers
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	// 精确查找
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.registrationMap[k])
	}
	// 模糊查找
	results := make(map[string]struct{}) // 这个results是去重用的,value采用空结构体可以省内存
	var retProducers Producers
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			_, fount := results[producer.peerInfo.id]
			if fount == false {
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

// 根据 producer.peerInfo.id 找Registrations
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.Unlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		if _, exists := producers[id]; exists {
			results = append(results, k)
		}
	}
	return results
}

func ProducerMap2Slice(pm ProducerMap) Producers {
	var producers Producers
	for _, producer := range pm {
		producers = append(producers, producer)
	}
	return producers
}
