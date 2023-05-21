package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"time"
)

type result struct {
	ThreadName string
	Message    *sqs.Message
}

func main() {
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

	// メッセージ送受信用チャネル
	chnMessage := make(chan *result, 10)

	// メッセージ受信を2スレッドで待ち受ける
	go receiveMessages("Thread1", svc, urlResult, chnMessage)
	go receiveMessages("Thread2", svc, urlResult, chnMessage)

	// 受信メッセージの処理
	for m := range chnMessage {
		gid := m.Message.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]
		fmt.Printf("[%s] Message ID: %s, MessageGroupID: %s, Body: %s\n", m.ThreadName, *m.Message.MessageId, *gid, *m.Message.Body)

		// 1メッセージの処理に、1秒かかると場合
		// time.Sleep(1000 * time.Millisecond)

		// この for loop はすべて同期処理とすべき、以下はテストのため goroutine を起動
		go deleteMessage(gid, m.Message, svc, urlResult)
	}
}

func receiveMessages(threadName string, svc *sqs.SQS, urlResult *sqs.GetQueueUrlOutput, chn chan<- *result) {
	var timeout int64
	timeout = 30
	for {
		msgResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			// 受け取りたい情報を指定(MessageGroupId など)
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
				aws.String(sqs.MessageSystemAttributeNameMessageGroupId),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            urlResult.QueueUrl,
			VisibilityTimeout:   &timeout,
			MaxNumberOfMessages: aws.Int64(3),
			WaitTimeSeconds:     aws.Int64(5),
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("[%s] %d messages received. \n", threadName, len(msgResult.Messages))
		for _, m := range msgResult.Messages {
			// 受け取ったメッセージをチャネルに送信
			chn <- &result{
				ThreadName: threadName,
				Message:    m,
			}
		}
	}
}

func deleteMessage(gid *string, m *sqs.Message, svc *sqs.SQS, urlResult *sqs.GetQueueUrlOutput) {
	if *gid == "group-2" && *m.Body == "Test Msg: 0" {
		// group-2 の1つ目だけ時間がかかると想定
		// 15秒経過するまでは group-2 のメッセージは読み出されないが、他のグループのメッセージは読み出されることを確認するため
		fmt.Printf(" - [START] 削除15秒まち. Message ID: %s, MessageGroupID: %s, Body: %s\n", *m.MessageId, *gid, *m.Body)
		time.Sleep(3 * time.Second)
		fmt.Printf(" - [END] 削除実行. Message ID: %s, MessageGroupID: %s, Body: %s\n", *m.MessageId, *gid, *m.Body)
	}
	svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      urlResult.QueueUrl,
		ReceiptHandle: m.ReceiptHandle,
	})
}
