package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	// 주요 작업 큐 생성
	mainQueue := make(chan event, mainQueueSize) // TODO 이 큐는 차오르면 문제 - 모니터링 필요

	// 메인에 넣는 고루틴 기다리기 위해 변수 생성
	var producerWaitGroup sync.WaitGroup

	// os signal 구독
	sigs := make(chan os.Signal, 1)
	workerDone := make(chan struct{})
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM) // 완료 보장 우선. 후속 종료 신호는 무시하고 끝까지 기다립니다
	producerShutdown := make(chan struct{})
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		close(producerShutdown)
	}()

	// 주요 틱 타이머 on
	start := time.Now()

	// 주요 작업 처리 고루틴
	var testCount int
	go func() {
		for eventCommand := range mainQueue {
			switch eventCommand.kind {
			case Ping:
				// 핑에 응답한다

			// TODO 만들어야 함
			//case 유저가 이동키 Pressed:
			//	// 유저를 이동시키기 시작한다

			case ServerTick:
				// 서버틱마다 처리해야 하는 작업들 처리 - 시뮬레이션의 본질, 예를 들면 이동 처리
				fmt.Print("틱")
				testCount++

			default:
				// 테스트 서버에서는 에러. 프로덕션 서버에서는 로깅/무시
			}
		}

		// graceful drain 마무리
		elapsed := time.Since(start)
		fmt.Printf("메인 writer 종료 경과 %vs, tick %v회, 실효 %v\n", elapsed, testCount, float64(testCount)/elapsed.Seconds())
		close(workerDone)
		return
	}()

	// 서버 메인 틱. 틱도 사용자 입력이 새치기 당하지 않게 큐를 거친다.
	// 주요 작업 처리 고루틴보다 뒤에서 시작해야함
	ticker := time.NewTicker(time.Second / serverTickRate)
	producerWaitGroup.Add(1)
	go func() {
		defer producerWaitGroup.Done()
		defer ticker.Stop()

		for {
			select {
			case <-producerShutdown:
				fmt.Print("메인 티커 종료 요청 받음\n") // TODO 메인큐 병렬에서 같아야 하는 코드
				return
			case <-ticker.C:
				select {
				case mainQueue <- event{
					kind: ServerTick,
				}:
				case <-producerShutdown:
					fmt.Print("메인 티커 종료 요청 받음\n")
					return
				}
			}

		}
	}()

	// TODO 유저 연결 받는 어셉터 고루틴. wg에

	// '주요 작업 처리 고루틴' 종료해도 된다는 고루틴
	go func() {
		// 메인 큐 입력 고루틴 모두 종료됐는지 확인
		producerWaitGroup.Wait()
		fmt.Printf("메인 종료 루틴\n")

		// 입력 고루틴 없으니 채널 닫음
		close(mainQueue)
	}()

	// gracefully shutdown
	<-workerDone
	fmt.Print("gracefully shutdown 완료\n")
}
