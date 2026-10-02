package main

type event struct {
	kind command
}

type command int

const (
	Ping       command = iota // 단순히 연결 확인용
	ServerTick                // 서버 처리 주기
)
