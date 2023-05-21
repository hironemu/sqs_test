package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"time"
)

func main() {
	fmt.Println("main1")
	queue := "queue.fifo"

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-northeast-1"),
		Credentials: credentials.NewSharedCredentials("", "hironemu"),
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	svc := sqs.New(sess)
	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	wg := new(sync.WaitGroup)

	// 3つの MessageGroupId を作る
	// 1グループ6個のメッセージを送信（各メッセージは 500-1000 ms のランダムな間隔で送信）
	for i := 0; i < 3; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		wg.Add(1)
		go func(groupID string) {
			for j := 0; j < 6; j++ {
				// sleep random millisecond
				n := rand.Intn(500)
				if groupID == "group-2" {
					// group-2 だけ差をつけるために早めに処理をする (0-500ms のランダムな間隔で送信)
					time.Sleep(time.Duration(n) * time.Millisecond)
				} else {
					time.Sleep(time.Duration(500+n) * time.Millisecond)
				}

				deduplicationId, _ := uuid.NewUUID()
				body := fmt.Sprintf("Test Msg: %d", j)
				_, err = svc.SendMessage(&sqs.SendMessageInput{
					MessageBody:            aws.String(body),
					QueueUrl:               urlResult.QueueUrl,
					MessageGroupId:         &groupID,
					MessageDeduplicationId: aws.String(deduplicationId.String()),
				})
				fmt.Printf("MessageGroupID: %s, Body: %s\n", groupID, body)
				if err != nil {
					fmt.Println(err)
					wg.Done()
					return
				}
			}
			wg.Done()
		}(groupID)
	}
	wg.Wait()
}
