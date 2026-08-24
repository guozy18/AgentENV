package scheduler

import "testing"

func TestSchedulerRPCLabelIncludesP2PControlPlaneRPCs(t *testing.T) {
	tests := []struct {
		method string
		want   string
	}{
		{method: "/agentenv.scheduler.Scheduler/ListP2pPeers", want: "ListP2PPeers"},
		{method: "/agentenv.scheduler.Scheduler/RecordP2pArtifact", want: "RecordP2PArtifact"},
		{method: "/agentenv.scheduler.Scheduler/ForgetP2pArtifact", want: "ForgetP2PArtifact"},
		{method: "/agentenv.scheduler.Scheduler/LookupP2pArtifact", want: "LookupP2PArtifact"},
		{method: "/agentenv.scheduler.Scheduler/Unknown", want: ""},
	}

	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			if got := schedulerRPCLabel(test.method); got != test.want {
				t.Fatalf("schedulerRPCLabel(%q) = %q, want %q", test.method, got, test.want)
			}
		})
	}
}
