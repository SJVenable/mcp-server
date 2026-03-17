package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonschema"
	mcp "github.com/modelcontextprotocol/go-sdk/mcp"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	taskruninformer "github.com/tektoncd/pipeline/pkg/client/injection/informers/pipeline/v1/taskrun"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
)

type getLogsParams struct {
	Name           string `json:"name"`
	Namespace      string `json:"namespace"`
	FailedOnly     bool   `json:"failed_only"`
	MaxLinesPerLog int    `json:"max_lines_per_log"`
}

func getTaskRunLogsSchema() (mcp.ToolOption, error) {
	scheme, err := jsonschema.For[getLogsParams]()
	if err != nil {
		return nil, err
	}

	scheme.Properties["name"].Description = "Name or referece of the object"
	scheme.Properties["namespace"].Description = "Namespace of the object"
	scheme.Properties["namespace"].Default = json.RawMessage(`"default"`)
	scheme.Properties["failed_only"].Description = "If true, only show full logs for failed containers/steps. Successful steps will show summary only."
	scheme.Properties["failed_only"].Default = json.RawMessage(`false`)
	scheme.Properties["max_lines_per_log"].Description = "Maximum number of lines to show per successful container log (0 = show all). Only applies when failed_only is true."
	scheme.Properties["max_lines_per_log"].Default = json.RawMessage(`10`)

	return mcp.Input(mcp.Schema(scheme)), nil
}

func getTaskRunLogs() (*mcp.ServerTool, error) {
	schema, err := getTaskRunLogsSchema()
	if err != nil {
		return nil, err
	}
	return mcp.NewServerTool(
		"get_taskrun_logs",
		"Get the logs for a given TaskRun",
		handlerGetTaskRunLogs,
		schema,
	), nil
}

func handlerGetTaskRunLogs(
	ctx context.Context,
	cc *mcp.ServerSession,
	params *mcp.CallToolParamsFor[getLogsParams],
) (*mcp.CallToolResultFor[string], error) {
	name := params.Arguments.Name
	namespace := params.Arguments.Namespace
	failedOnly := params.Arguments.FailedOnly
	maxLines := params.Arguments.MaxLinesPerLog

	taskrunInformer := taskruninformer.Get(ctx)
	kubeclientset := kubeclient.Get(ctx)

	task, err := taskrunInformer.Lister().TaskRuns(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get TaskRun %s/%s: %w", namespace, name, err)
	}

	podName := task.Status.PodName
	if podName == "" {
		return nil, fmt.Errorf("podName not set for TaskRun %s/%s", namespace, name)
	}

	logs, err := getLogs(ctx, kubeclientset.CoreV1().Pods(namespace), podName, task, failedOnly, maxLines)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs for TaskRun %s/%s: %w", namespace, name, err)
	}

	return result(logs), nil
}

func getLogs(ctx context.Context, client corev1.PodInterface, name string, taskRun *pipelinev1.TaskRun, failedOnly bool, maxLines int) (string, error) {
	pod, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get Pod %s: %w", name, err)
	}

	// Build a map of container statuses to determine which failed
	containerStatuses := make(map[string]*v1.ContainerStatus)
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		containerStatuses[cs.Name] = cs
	}

	var sb strings.Builder

	// Add TaskRun summary at the top
	sb.WriteString(fmt.Sprintf("=== TaskRun: %s ===\n", taskRun.Name))
	if taskRun.Status.CompletionTime != nil {
		sb.WriteString(fmt.Sprintf("Status: %s\n", getTaskRunStatus(taskRun)))
		sb.WriteString(fmt.Sprintf("Started: %s\n", taskRun.Status.StartTime.Format("2006-01-02 15:04:05")))
		sb.WriteString(fmt.Sprintf("Completed: %s\n", taskRun.Status.CompletionTime.Format("2006-01-02 15:04:05")))
	} else {
		sb.WriteString(fmt.Sprintf("Status: %s\n", getTaskRunStatus(taskRun)))
		if taskRun.Status.StartTime != nil {
			sb.WriteString(fmt.Sprintf("Started: %s\n", taskRun.Status.StartTime.Format("2006-01-02 15:04:05")))
		}
	}
	sb.WriteString("\n")

	for _, container := range pod.Spec.Containers {
		containerStatus := containerStatuses[container.Name]
		isInit := strings.HasPrefix(container.Name, "step-")

		// Determine if this container failed
		containerFailed := false
		if containerStatus != nil {
			if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
				containerFailed = true
			}
			if containerStatus.State.Waiting != nil && containerStatus.State.Waiting.Reason == "CrashLoopBackOff" {
				containerFailed = true
			}
		}

		// Header for the container
		statusIcon := "✓"
		if containerFailed {
			statusIcon = "✗"
		}

		sb.WriteString(fmt.Sprintf("\n>>> %s Pod %s Container %s\n", statusIcon, pod.Name, container.Name))

		// If failedOnly is true and this container succeeded, show summary only
		if failedOnly && !containerFailed {
			if containerStatus != nil && containerStatus.State.Terminated != nil {
				sb.WriteString(fmt.Sprintf("    Status: Succeeded (exit code: %d)\n", containerStatus.State.Terminated.ExitCode))
				if isInit {
					sb.WriteString("    [Log output truncated - step succeeded]\n")
				}
			} else {
				sb.WriteString("    Status: Running or Pending\n")
			}
			continue
		}

		// Get full logs for failed containers or when failedOnly is false
		req := client.GetLogs(pod.Name, &v1.PodLogOptions{Follow: false, Container: container.Name})
		res, err := req.Stream(ctx)
		if err != nil {
			sb.WriteString(fmt.Sprintf("    Error getting logs: %v\n", err))
			continue
		}

		data, err := io.ReadAll(res)
		res.Close()
		if err != nil {
			sb.WriteString(fmt.Sprintf("    Error reading logs: %v\n", err))
			continue
		}

		logContent := string(data)

		// If failedOnly is true and maxLines > 0, truncate successful container logs
		if failedOnly && !containerFailed && maxLines > 0 {
			lines := strings.Split(logContent, "\n")
			if len(lines) > maxLines {
				sb.WriteString(strings.Join(lines[:maxLines], "\n"))
				sb.WriteString(fmt.Sprintf("\n... [%d more lines truncated] ...\n", len(lines)-maxLines))
			} else {
				sb.WriteString(logContent)
			}
		} else {
			sb.WriteString(logContent)
		}

		if !strings.HasSuffix(logContent, "\n") {
			sb.WriteString("\n")
		}
	}

	return sb.String(), nil
}

// getTaskRunStatus returns a human-readable status for the TaskRun
func getTaskRunStatus(tr *pipelinev1.TaskRun) string {
	if tr.Status.Conditions == nil || len(tr.Status.Conditions) == 0 {
		return "Unknown"
	}

	cond := tr.Status.Conditions[0]
	if cond.Status == "True" {
		return "Succeeded"
	} else if cond.Status == "False" {
		if cond.Reason != "" {
			return fmt.Sprintf("Failed (%s)", cond.Reason)
		}
		return "Failed"
	}
	return "Running"
}
