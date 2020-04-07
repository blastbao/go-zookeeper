package zk

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (

	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = errors.New("zk: trying to acquire a lock twice")

	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked = errors.New("zk: not locked")
)

// Lock is a mutual exclusion lock.
type Lock struct {
	c        *Conn
	path     string		//
	acl      []ACL
	lockPath string
	seq      int
}

// NewLock creates a new lock instance using the provided connection, path, and acl.
// The path must be a node that is only used by this lock.
// A lock instances starts unlocked until Lock() is called.
func NewLock(c *Conn, path string, acl []ACL) *Lock {
	return &Lock{
		c:    c,
		path: path,
		acl:  acl,
	}
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

// Lock attempts to acquire the lock.
//
// It will wait to return until the lock is acquired or an error occurs.
// If this instance already has the lock then ErrDeadlock is returned.
func (l *Lock) Lock() error {

	if l.lockPath != "" {
		return ErrDeadlock
	}

	prefix := fmt.Sprintf("%s/lock-", l.path)
	path := ""
	var err error

	// 重试创建
	for i := 0; i < 3; i++ {

		// 创建临时、顺序节点，返回新节点路径
		path, err = l.c.CreateProtectedEphemeralSequential(prefix, []byte{}, l.acl)

		// 若父节点不存在，则循环创建父节点
		if err == ErrNoNode {
			// Create parent node.
			parts := strings.Split(l.path, "/")
			pth := ""
			for _, p := range parts[1:] {
				var exists bool
				pth += "/" + p
				exists, _, err = l.c.Exists(pth)
				if err != nil {
					return err
				}
				if exists == true {
					continue
				}
				_, err = l.c.Create(pth, []byte{}, 0, l.acl)
				if err != nil && err != ErrNodeExists {
					return err
				}
			}
		} else if err == nil {
			break
		} else {
			return err
		}
	}

	if err != nil {
		return err
	}

	// 获取临时节点序号
	seq, err := parseSeq(path)
	if err != nil {
		return err
	}


	for {

		// 获取 l.path 下的子节点
		children, _, err := l.c.Children(l.path)
		if err != nil {
			return err
		}

		lowestSeq := seq
		prevSeq := -1
		prevSeqPath := ""
		for _, p := range children {

			// 解析子节点 p 的序号 s
			s, err := parseSeq(p)

			// 出错则 return
			if err != nil {
				return err
			}

			// 更新最小序号
			if s < lowestSeq {
				lowestSeq = s
			}

			// 取出当前 seq 节点的前一个 prev 节点，只有当这个 prev 节点退出后，当前节点才有可能获得锁
			if s < seq && s > prevSeq {
				prevSeq = s
				prevSeqPath = p
			}
		}

		// 如果当前节点是序号最小的节点，则可以立即获得锁
		if seq == lowestSeq {
			// Acquired the lock
			break
		}


		// 否则，需要等待 prev 节点退出才有可能获得锁（但不是一定能获得），这里阻塞式的监听 prev 节点上的事件


		// Wait on the node next in line for the lock
		_, _, ch, err := l.c.GetW(l.path + "/" + prevSeqPath)
		if err != nil && err != ErrNoNode {
			return err
		} else if err != nil && err == ErrNoNode {
			// try again
			continue
		}

		// 如果 prev 节点上发生变更，则开始新一轮 for loop 检查
		ev := <-ch

		// 出错? 报错返回
		if ev.Err != nil {
			return ev.Err
		}

	}

	// 获得锁成功
	l.seq = seq
	l.lockPath = path //更新锁节点（临时、顺序节点）的路径，被用于解锁
	return nil
}

// Unlock releases an acquired lock.
//
// If the lock is not currently acquired by this Lock instance than ErrNotLocked is returned.
func (l *Lock) Unlock() error {

	if l.lockPath == "" {
		return ErrNotLocked
	}

	if err := l.c.Delete(l.lockPath, -1); err != nil {
		return err
	}
	l.lockPath = ""
	l.seq = 0
	return nil
}
