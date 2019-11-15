package bokchoy

import (
	"context"
	"fmt"
	"time"

	"github.com/thoas/bokchoy/logging"

	"github.com/pkg/errors"
)

// Queue contains consumers to enqueue.
type Queues struct {
	broker Broker

	queues []*Queue
}

// Publish publishes a new payload to the queue.
func (q *Queues) Publish(ctx context.Context, payload interface{}, options ...Option) ([]*Task, error) {
	tasks := make([]*Task, 0, len(q.queues))
	for _, queue := range q.queues {
		tasks = append(tasks, queue.NewTask(payload, options...))
	}

	err := q.PublishTasks(ctx, tasks)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// PublishTask publishes a new task to the queue.
func (q *Queues) PublishTasks(ctx context.Context, tasks []*Task) error {
	data := make([]map[string]interface{}, 0, len(q.queues))
	queueNames := make([]string, 0, len(q.queues))
	taskPrefixes := make([]string, 0, len(q.queues))
	taskIds := make([]string, 0, len(q.queues))
	taskETAs := make([]time.Time, 0, len(q.queues))

	start := time.Now()

	i := 0
	for _, task := range tasks {
		queue := q.queues[i]
		d, err := task.Serialize(queue.serializer)
		if err != nil {
			return err
		}
		data = append(data, d)

		// if eta is after now then it should be a delayed task
		if !task.ETA.IsZero() && task.ETA.After(time.Now().UTC()) {
			queueNames = append(queueNames, queue.DelayName())
		} else {
			queueNames = append(queueNames, queue.name)
		}

		taskPrefixes = append(taskPrefixes, fmt.Sprintf("%s:", queue.name))
		taskETAs = append(taskETAs, task.ETA)
		taskIds = append(taskIds, task.ID)
		i++
	}

	err := q.broker.PublishMulti(queueNames, taskPrefixes, taskIds, data, taskETAs)
	if err != nil {
		return errors.Wrapf(err, "unable to publish tasks %v", tasks)
	}

	i = 0
	for _, task := range tasks {
		queue := q.queues[i]
		queue.logger.Debug(ctx, "Task published",
			logging.Object("queue", queue),
			logging.Duration("duration", time.Since(start)),
			logging.Object("task", task))

		i++
	}

	return nil
}
